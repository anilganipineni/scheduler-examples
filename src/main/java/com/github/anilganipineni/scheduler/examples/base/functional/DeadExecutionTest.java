package com.github.anilganipineni.scheduler.examples.base.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.anilganipineni.scheduler.ExecutionComplete;
import com.github.anilganipineni.scheduler.ExecutionContext;
import com.github.anilganipineni.scheduler.ExecutionOperations;
import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.StatsRegistry.SchedulerStatsEvent;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.examples.base.EmbeddedPostgresqlExtension;
import com.github.anilganipineni.scheduler.examples.base.StopSchedulerExtension;
import com.github.anilganipineni.scheduler.examples.base.helper.TestableRegistry;
import com.github.anilganipineni.scheduler.schedule.Clock;
import com.github.anilganipineni.scheduler.task.CustomTask;
import com.github.anilganipineni.scheduler.task.TaskFactory;
import com.github.anilganipineni.scheduler.task.handler.CompletionHandler;
import com.github.anilganipineni.scheduler.task.handler.ExecutionHandler;

public class DeadExecutionTest {

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();
    @RegisterExtension
    public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

    @Test
    public void test_dead_execution() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
        	ExecutionHandler executionHandler = new ExecutionHandler() {
				/**
				 * @param scheduler
				 * @param clock
				 */
				@Override
				public void onStartup(Scheduler scheduler, Clock clock) {
					// TODO Auto-generated method stub
				}
				/**
				 * @param task
				 * @param context
				 * @return
				 */
				@Override
				public CompletionHandler execute(ScheduledTasks task, ExecutionContext context) {
					return new CompletionHandler() {
						/**
						 * @param executionComplete
						 * @param executionOperations
						 */
						@Override
						public void complete(ExecutionComplete executionComplete, ExecutionOperations executionOperations) {
	                        //do nothing on complete, row will be left as-is in database
						}
	                };
				}
        	};
            CustomTask customTask = TaskFactory.custom("custom-a").execute(executionHandler);

            TestableRegistry.Condition completedCondition = TestableRegistry.Conditions.completed(2);

            TestableRegistry registry = TestableRegistry.create().waitConditions(completedCondition).build();

            Scheduler scheduler = Scheduler.create(postgres.getSchedulerDataSource(), customTask)
                .pollingInterval(Duration.ofMillis(100))
                .heartbeatInterval(Duration.ofMillis(100))
                .statsRegistry(registry)
                .build();
            stopScheduler.register(scheduler);

            scheduler.schedule(customTask.instance("1"), Instant.now());
            scheduler.start();
            completedCondition.waitFor();

            assertEquals(registry.getCount(SchedulerStatsEvent.DEAD_EXECUTION), 1);

        });
    }

}
