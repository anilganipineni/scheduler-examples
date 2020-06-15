package com.github.anilganipineni.scheduler.examples.base.example;

import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.examples.base.HsqlTestDatabaseExtension;
import com.github.anilganipineni.scheduler.task.OneTimeTask;
import com.github.anilganipineni.scheduler.task.TaskFactory;

public class MaxRetriesMain {
    private static final Logger LOG = LoggerFactory.getLogger(MaxRetriesMain.class);

    private static void example(SchedulerDataSource dataSource) {

        OneTimeTask failingTask = TaskFactory.oneTime("max_retries_task").onFailure((executionComplete, executionOperations) -> {

                    if (executionComplete.getExecution().getConsecutiveFailures() > 3) {
                        System.out.println("Execution has failed " + executionComplete.getExecution().getConsecutiveFailures() + " times. Cancelling execution.");
                        executionOperations.stop();
                    } else {
                        // try again in 1 second
                        System.out.println("Execution has failed " + executionComplete.getExecution().getConsecutiveFailures() + " times. Trying again in a bit...");
                        executionOperations.reschedule(executionComplete, Instant.now().plusSeconds(1));
                    }
                })
                .execute((taskInstance, executionContext) -> {
                    throw new RuntimeException("simulated task exception");
                });

        final Scheduler scheduler = Scheduler
                .create(dataSource, failingTask)
                .pollingInterval(Duration.ofSeconds(2))
                .build();

        scheduler.schedule(failingTask.instance("1"), Instant.now());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Received shutdown signal.");
            scheduler.stop();
        }));

        scheduler.start();
    }

    public static void main(String[] args) throws Throwable {
        try {
            final HsqlTestDatabaseExtension hsqlRule = new HsqlTestDatabaseExtension();
            hsqlRule.beforeEach(null);

            example(hsqlRule);
        } catch (Exception e) {
            LOG.error("Error", e);
        }

    }

}
