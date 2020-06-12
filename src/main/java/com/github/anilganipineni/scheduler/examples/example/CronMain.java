package com.github.anilganipineni.scheduler.examples.example;

import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.examples.HsqlTestDatabaseExtension;
import com.github.anilganipineni.scheduler.schedule.Schedule;
import com.github.anilganipineni.scheduler.schedule.ScheduleFactory;
import com.github.anilganipineni.scheduler.task.RecurringTask;
import com.github.anilganipineni.scheduler.task.TaskFactory;

public class CronMain {
    private static final Logger LOG = LoggerFactory.getLogger(CronMain.class);

    private static void example(SchedulerDataSource dataSource) {

        Schedule cron = ScheduleFactory.cron("*/10 * * * * ?");
        RecurringTask cronTask = TaskFactory.recurring("cron-task", cron)
                .execute((taskInstance, executionContext) -> {
                    System.out.println(Instant.now().getEpochSecond() + "s  -  Cron-schedule!");
                });

        final Scheduler scheduler = Scheduler
                .create(dataSource)
                .startTasks(cronTask)
                .pollingInterval(Duration.ofSeconds(1))
                .build();

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
