package com.github.anilganipineni.scheduler.examples.base.example;

import java.io.Serializable;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.anilganipineni.scheduler.Scheduler;
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

        final Scheduler scheduler = Scheduler
                .create(dataSource)
                .startTasks(hourlyTask)
                .threads(5)
                .build();

        // hourlyTask is automatically scheduled on startup if not already started (i.e. exists in the db)
        scheduler.start();
    }

    private static void adhocTask(SchedulerDataSource dataSource) {

        OneTimeTask myAdhocTask = TaskFactory.oneTime("my-typed-adhoc-task")
                .execute((inst, ctx) -> {
                    System.out.println("Executed! Custom data, Id: " + inst.getId());
                });

        final Scheduler scheduler = Scheduler
                .create(dataSource, myAdhocTask)
                .threads(5)
                .build();

        scheduler.start();

        // Schedule the task for execution a certain time in the future and optionally provide custom data for the execution
        scheduler.schedule(myAdhocTask.instance("1045", new MyTaskData(1001L)), Instant.now().plusSeconds(5));
    }
    /**
     * @author akganipineni
     */
    @SuppressWarnings("serial")
	public static class MyTaskData implements Serializable {
        public final long id;

        public MyTaskData(long id) {
            this.id = id;
        }
    }
}
