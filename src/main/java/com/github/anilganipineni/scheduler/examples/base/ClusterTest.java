package com.github.anilganipineni.scheduler.examples.base;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.anilganipineni.scheduler.ExecutionComplete;
import com.github.anilganipineni.scheduler.ExecutionOperations;
import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.SchedulerBuilder;
import com.github.anilganipineni.scheduler.SchedulerName;
import com.github.anilganipineni.scheduler.schedule.ScheduleFactory;
import com.github.anilganipineni.scheduler.task.RecurringTask;
import com.github.anilganipineni.scheduler.task.Task;
import com.github.anilganipineni.scheduler.task.TaskFactory;
import com.github.anilganipineni.scheduler.task.handler.CompletionHandler;
import com.google.common.collect.Lists;


public class ClusterTest {

    @RegisterExtension
    public EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();
    @RegisterExtension
    public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

    @Test
    public void test_concurrency() throws InterruptedException {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

            final List<String> ids = IntStream.range(1, 1001).mapToObj(String::valueOf).collect(toList());

            final CountDownLatch completeAllIds = new CountDownLatch(ids.size());
            final RecordResultAndStopExecutionOnComplete completed = new RecordResultAndStopExecutionOnComplete(
                (id) -> completeAllIds.countDown());
            final Task task = ComposableTask.customTask("Custom", completed, new TestTasks.SleepingHandler(1));

            final TestTasks.SimpleStatsRegistry stats = new TestTasks.SimpleStatsRegistry();
            final Scheduler scheduler1 = createScheduler("scheduler1", task, stats);
            final Scheduler scheduler2 = createScheduler("scheduler2", task, stats);

            stopScheduler.register(scheduler1, scheduler2);
            scheduler1.start();
            scheduler2.start();

            ids.forEach(id -> {
                scheduler1.schedule(task.instance(id), Instant.now());
            });

            completeAllIds.await();
            scheduler1.stop();
            scheduler2.stop();

            assertThat(completed.failed.size(), is(0));
            assertThat(completed.ok.size(), is(ids.size()));
            assertThat("Should contain no duplicates", new HashSet<>(completed.ok).size(), is(ids.size()));
            assertThat(stats.unexpectedErrors.get(), is(0));
            assertThat(scheduler1.getCurrentlyExecuting(), hasSize(0));
            assertThat(scheduler2.getCurrentlyExecuting(), hasSize(0));
        });

    }

    @Test
    public void test_concurrency_recurring() throws InterruptedException {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

            final RecurringTask task1 = TaskFactory.recurring("task1", ScheduleFactory.fixedDelay(Duration.ofMillis(0)))
                .execute((taskId, executionContext) -> {
                    // do nothing
                    // System.out.println(counter.incrementAndGet() + " " + Thread.currentThread().getName());
                });

            final TestTasks.SimpleStatsRegistry stats = new TestTasks.SimpleStatsRegistry();
            final Scheduler scheduler1 = createSchedulerRecurring("scheduler1", task1, stats);
            final Scheduler scheduler2 = createSchedulerRecurring("scheduler2", task1, stats);

            stopScheduler.register(scheduler1, scheduler2);
            scheduler1.start();
            scheduler2.start();

            Thread.sleep(5_000);
            scheduler1.stop();
            scheduler2.stop();
            assertThat(stats.unexpectedErrors.get(), is(0));
            assertThat(scheduler1.getCurrentlyExecuting(), hasSize(0));
            assertThat(scheduler2.getCurrentlyExecuting(), hasSize(0));
        });
    }

    private Scheduler createScheduler(String name, Task task, TestTasks.SimpleStatsRegistry stats) {
        return SchedulerBuilder.create(DB.getSchedulerDataSource(), Lists.newArrayList(task))
                .schedulerName(new SchedulerName.Fixed(name)).pollingInterval(Duration.ofMillis(0))
                .heartbeatInterval(Duration.ofMillis(100)).statsRegistry(stats).build();
    }

    private Scheduler createSchedulerRecurring(String name, RecurringTask task, TestTasks.SimpleStatsRegistry stats) {
        return SchedulerBuilder.create(DB.getSchedulerDataSource())
            .startTasks(task)
            .schedulerName(new SchedulerName.Fixed(name))
            .pollingInterval(Duration.ofMillis(0))
            .heartbeatInterval(Duration.ofMillis(100))
            .statsRegistry(stats).build();
    }

    private static class RecordResultAndStopExecutionOnComplete implements CompletionHandler {

        private final List<String> ok = Collections.synchronizedList(new ArrayList<>());
        private final List<String> failed = Collections.synchronizedList(new ArrayList<>());
        private final Consumer<String> onComplete;

        RecordResultAndStopExecutionOnComplete(Consumer<String> onComplete) {
            this.onComplete = onComplete;
        }

        @Override
        public void complete(ExecutionComplete executionComplete, ExecutionOperations executionOperations) {
            final String instanceId = executionComplete.getExecution().getTaskId();
            if (executionComplete.getResult() == ExecutionComplete.Result.OK) {
                ok.add(instanceId);
            } else {
                failed.add(instanceId);
            }
            executionOperations.stop();
            onComplete.accept(instanceId);
        }
    }

}
