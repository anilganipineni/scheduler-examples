package com.github.anilganipineni.scheduler.examples.base.example;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.SchedulerBuilder;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.examples.base.HsqlTestDatabaseExtension;
import com.github.anilganipineni.scheduler.schedule.FixedDelay;
import com.github.anilganipineni.scheduler.task.OneTimeTask;
import com.github.anilganipineni.scheduler.task.RecurringTask;
import com.github.anilganipineni.scheduler.task.TaskFactory;

public class TasksMain {
    private static final Logger LOG = LoggerFactory.getLogger(TasksMain.class);

    public static void main(String[] args) throws Throwable {
        try {
            final HsqlTestDatabaseExtension hsqlRule = new HsqlTestDatabaseExtension();
            hsqlRule.beforeEach(null);
            // recurringTask(dataSource);
            adhocTask(hsqlRule);
        } catch (Exception e) {
            LOG.error("Error", e);
        }
    }

    protected static void recurringTask(SchedulerDataSource dataSource) {

        RecurringTask hourlyTask = TaskFactory.recurring("my-hourly-task", FixedDelay.ofHours(1))
                .execute((inst, ctx) -> {
                    System.out.println("Executed!");
                });

        final Scheduler scheduler = SchedulerBuilder.create(dataSource)
                .startTasks(hourlyTask)
                .threads(5)
                .build();

        // hourlyTask is automatically scheduled on startup if not already started (i.e. exists in the db)
        scheduler.start();
    }

    private static void adhocTask(SchedulerDataSource dataSource) {

        OneTimeTask myAdhocTask = TaskFactory.oneTime("my-typed-adhoc-task")
                .execute((inst, ctx) -> {
                    System.out.println("Executed! Custom data, Id: " + inst.getTaskId());
                });

        final Scheduler scheduler = SchedulerBuilder.create(dataSource, myAdhocTask)
                .threads(5)
                .build();

        scheduler.start();
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("id", Long.parseLong("1001"));

        // Schedule the task for execution a certain time in the future and optionally provide custom data for the execution
        scheduler.schedule(myAdhocTask.instance("1045", data), Instant.now().plusSeconds(5));
    }
}
