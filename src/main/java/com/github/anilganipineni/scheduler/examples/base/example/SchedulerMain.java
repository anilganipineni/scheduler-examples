package com.github.anilganipineni.scheduler.examples.base.example;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.examples.base.HsqlTestDatabaseExtension;
import com.github.anilganipineni.scheduler.schedule.FixedDelay;
import com.github.anilganipineni.scheduler.task.OneTimeTask;
import com.github.anilganipineni.scheduler.task.RecurringTask;
import com.github.anilganipineni.scheduler.task.TaskFactory;

public class SchedulerMain {
    private static final Logger LOG = LoggerFactory.getLogger(SchedulerMain.class);

    private static void example(SchedulerDataSource dataSource) {

        // recurring with no data
        RecurringTask recurring1 = TaskFactory.recurring("recurring_no_data", FixedDelay.of(Duration.ofSeconds(5)))
                .onFailureReschedule()   // default
                .onDeadExecutionRevive() // default
                .execute((taskInstance, executionContext) -> {
                    sleep(100);
                    System.out.println("Executing " + taskInstance.getTaskAndInstance());
                });

        // recurring with contant data
        RecurringTask recurring2 = TaskFactory.recurring("recurring_constant_data", FixedDelay.of(Duration.ofSeconds(7)))
                .initialData(1)
                .onFailureReschedule()   // default
                .onDeadExecutionRevive() // default
                .execute((taskInstance, executionContext) -> {
                    sleep(100);
                    System.out.println("Executing " + taskInstance.getTaskAndInstance() + " , data: " + taskInstance.getTaskData());
                });

        // recurring with changing data
        /*Schedule custom1Schedule = FixedDelay.of(Duration.ofSeconds(4));
        CustomTask custom1 = TaskFactory.custom("recurring_changing_data")
                .scheduleOnStartup("instance1", 1, custom1Schedule::getInitialExecutionTime)
                .onFailureReschedule(custom1Schedule)  // default
                .onDeadExecutionRevive()               // default
                .execute((taskInstance, executionContext) -> {

                    System.out.println("Executing " + taskInstance.getTaskAndInstance() + " , data: " + taskInstance.getData());
                    return (executionComplete, executionOperations) -> {
                        sleep(100);
                        Instant nextExecutionTime = custom1Schedule.getNextExecutionTime(executionComplete);
                        int newData = taskInstance.getData() + 1;
                        executionOperations.reschedule(executionComplete, nextExecutionTime, newData);
                    };
                });*/

        // one-time with no data
        OneTimeTask onetime1 = TaskFactory.oneTime("onetime_no_data")
                .onDeadExecutionRevive()  // default
                .onFailureRetryLater()    // default
                .execute((taskInstance, executionContext) -> {
                    sleep(100);
                    System.out.println("Executing " + taskInstance.getTaskAndInstance());
                });

        // one-time with data
        OneTimeTask onetime2 = TaskFactory.oneTime("onetime_withdata")
                .onFailureRetryLater()    // default
                .execute((taskInstance, executionContext) -> {
                    sleep(100);
                    System.out.println("Executing " + taskInstance.getTaskAndInstance() + " , data: " + taskInstance.getTaskData());
                });


        final Scheduler scheduler = Scheduler
                .create(dataSource, onetime1, onetime2)
                .startTasks(Arrays.asList(recurring1, recurring2))
                .build();


        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOG.info("Received shutdown signal.");
                scheduler.stop();
            }
        });

        scheduler.start();

        sleep(3000);

        scheduler.schedule(onetime1.instance("onetime1_directly"), Instant.now());
        scheduler.schedule(onetime2.instance("onetime2", 100), Instant.now().plusSeconds(3));

        scheduler.schedule(onetime2.instance("onetime3", 100), Instant.now());
    }

    private static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
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
