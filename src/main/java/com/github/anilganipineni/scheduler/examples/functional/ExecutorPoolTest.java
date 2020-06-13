package com.github.anilganipineni.scheduler.examples.functional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

import org.hamcrest.core.Is;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.anilganipineni.scheduler.ExecutionComplete;
import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.examples.EmbeddedPostgresqlExtension;
import com.github.anilganipineni.scheduler.examples.StopSchedulerExtension;
import com.github.anilganipineni.scheduler.examples.TestTasks;
import com.github.anilganipineni.scheduler.examples.helper.TestableRegistry;
import com.github.anilganipineni.scheduler.task.OneTimeTask;
import com.github.anilganipineni.scheduler.testhelper.SettableClock;

import co.unruly.matchers.TimeMatchers;


public class ExecutorPoolTest {
    private SettableClock clock;

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();
    @RegisterExtension
    public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

    @BeforeEach
    public void setUp() {
        clock = new SettableClock();

    }

    // TODO: flaky test
    @Test
    public void test_execute_until_none_left_happy() {
        testExecuteUntilNoneLeft(2, 2, 20);
    }

    @Test
    public void test_execute_until_none_left_low_polling_limit() {
        testExecuteUntilNoneLeft(2, 10, 20);
    }

    @Test
    @Disabled //FIXLATER: Disabled because of flakiness. Need to investigate and re-enable 
    public void test_execute_until_none_left_high_volume() {
        testExecuteUntilNoneLeft(12, 4, 200);
    }


    private void testExecuteUntilNoneLeft(int pollingLimit, int threads, int executionsToRun) {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {

            Instant now = Instant.now();
            OneTimeTask task = TestTasks.oneTime("onetime-a", TestTasks.DO_NOTHING);
            TestableRegistry.Condition condition = TestableRegistry.Conditions.completed(executionsToRun);
            TestableRegistry registry = TestableRegistry.create().waitConditions(condition).build();

            Scheduler scheduler = Scheduler.create(postgres.getSchedulerDataSource(), task)
                .pollingLimit(pollingLimit)
                .threads(threads)
                .pollingInterval(Duration.ofMinutes(1))
                .statsRegistry(registry)
                .build();
            stopScheduler.register(scheduler);

            IntStream.range(0, executionsToRun).forEach(i -> scheduler.schedule(task.instance(String.valueOf(i)), clock.now()));

            scheduler.start();
            condition.waitFor();

            List<ExecutionComplete> completed = registry.getCompleted();
            assertThat(completed, hasSize(executionsToRun));
            completed.stream().forEach(e -> {
                assertThat(e.getResult(), Is.is(ExecutionComplete.Result.OK));
                Duration durationUntilExecuted = Duration.between(now, e.getTimeDone());
                assertThat(durationUntilExecuted, TimeMatchers.shorterThan(Duration.ofSeconds(1)));
            });
            registry.assertNoFailures();
        });
    }

}
