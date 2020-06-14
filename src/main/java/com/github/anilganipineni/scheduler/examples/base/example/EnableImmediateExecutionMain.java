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

public class EnableImmediateExecutionMain {
    private static final Logger LOG = LoggerFactory.getLogger(EnableImmediateExecutionMain.class);

    private static void example(SchedulerDataSource dataSource) {

        OneTimeTask onetimeTask = TaskFactory.oneTime("my_task")
                .execute((taskInstance, executionContext) -> {
                    System.out.println("Executed!");
                });

        final Scheduler scheduler = Scheduler
                .create(dataSource, onetimeTask)
                .pollingInterval(Duration.ofSeconds(5))
                .enableImmediateExecution()
                .build();


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Received shutdown signal.");
            scheduler.stop();
        }));

        scheduler.start();

        sleep(2);
        System.out.println("Scheduling task to executed immediately.");
        scheduler.schedule(onetimeTask.instance("1"), Instant.now());
//        scheduler.triggerCheckForDueExecutions();  // another option for triggering execution directly
    }

    private static void sleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
