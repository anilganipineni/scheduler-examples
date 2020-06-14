package com.github.anilganipineni.scheduler.examples.base.functional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.hamcrest.core.Is;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.anilganipineni.scheduler.ExecutionComplete;
import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.examples.base.EmbeddedPostgresqlExtension;
import com.github.anilganipineni.scheduler.examples.base.StopSchedulerExtension;
import com.github.anilganipineni.scheduler.examples.base.TestTasks;
import com.github.anilganipineni.scheduler.examples.base.helper.SettableClock;
import com.github.anilganipineni.scheduler.examples.base.helper.TestableRegistry;
import com.github.anilganipineni.scheduler.task.OneTimeTask;

import co.unruly.matchers.TimeMatchers;


public class ImmediateExecutionTest {

    private SettableClock clock;

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();
    @RegisterExtension
    public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

    @BeforeEach
    public void setUp() {
        clock = new SettableClock();
    }

    @Test
    public void test_immediate_execution() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

            Instant now = Instant.now();
            OneTimeTask task = TestTasks.oneTime("onetime-a", TestTasks.DO_NOTHING);
            TestableRegistry.Condition completedCondition = TestableRegistry.Conditions.completed(1);
            TestableRegistry.Condition executeDueCondition = TestableRegistry.Conditions.ranExecuteDue(1);

            TestableRegistry registry = TestableRegistry.create().waitConditions(executeDueCondition, completedCondition).build();

            Scheduler scheduler = Scheduler.create(postgres.getSchedulerDataSource(), task)
                .pollingInterval(Duration.ofMinutes(1))
                .enableImmediateExecution()
                .statsRegistry(registry)
                .build();
            stopScheduler.register(scheduler);

            scheduler.start();
            executeDueCondition.waitFor();

            scheduler.schedule(task.instance("1"), clock.now());
            completedCondition.waitFor();

            List<ExecutionComplete> completed = registry.getCompleted();
            assertThat(completed, hasSize(1));
            completed.stream().forEach(e -> {
                assertThat(e.getResult(), Is.is(ExecutionComplete.Result.OK));
                Duration durationUntilExecuted = Duration.between(now, e.getTimeDone());
                assertThat(durationUntilExecuted, TimeMatchers.shorterThan(Duration.ofSeconds(1)));
            });
            registry.assertNoFailures();
        });
    }

}
