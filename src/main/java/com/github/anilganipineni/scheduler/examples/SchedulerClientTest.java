package com.github.anilganipineni.scheduler.examples;

import static java.time.Duration.ofSeconds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.anilganipineni.scheduler.ScheduledExecution;
import com.github.anilganipineni.scheduler.SchedulerClient;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.examples.TestTasks.SavingHandler;
import com.github.anilganipineni.scheduler.task.OneTimeTask;
import com.github.anilganipineni.scheduler.task.handler.VoidExecutionHandler;
import com.github.anilganipineni.scheduler.task.helper.ExecutionContext;
import com.github.anilganipineni.scheduler.testhelper.ManualScheduler;
import com.github.anilganipineni.scheduler.testhelper.SettableClock;
import com.github.anilganipineni.scheduler.testhelper.TestHelper;

import co.unruly.matchers.OptionalMatchers;


public class SchedulerClientTest {

    @RegisterExtension
    public EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();

    private ManualScheduler scheduler;
    private SettableClock settableClock;

    private TestTasks.CountingHandler onetimeTaskHandlerA;
    private OneTimeTask oneTimeTaskA;

    private TestTasks.CountingHandler onetimeTaskHandlerB;
    private OneTimeTask oneTimeTaskB;

    private ScheduleAnotherTaskHandler scheduleAnother;
    private OneTimeTask scheduleAnotherTask;

    private SavingHandler savingHandler;
    private OneTimeTask savingTask;


    @BeforeEach
    public void setUp() {
        settableClock = new SettableClock();

        onetimeTaskHandlerA = new TestTasks.CountingHandler();
        oneTimeTaskA = TestTasks.oneTime("OneTimeA", onetimeTaskHandlerA);

        oneTimeTaskB = TestTasks.oneTime("OneTimeB", onetimeTaskHandlerB);
        onetimeTaskHandlerB = new TestTasks.CountingHandler();

        scheduleAnother = new ScheduleAnotherTaskHandler(oneTimeTaskA.instance("secondTask"), settableClock.now().plusSeconds(1));
        scheduleAnotherTask = TestTasks.oneTime("ScheduleAnotherTask", scheduleAnother);

        savingHandler = new SavingHandler();
        savingTask = TestTasks.oneTime("SavingTask", savingHandler);

        scheduler = TestHelper.createManualScheduler(DB.getSchedulerDataSource(), oneTimeTaskA, oneTimeTaskB, scheduleAnotherTask, savingTask).clock(settableClock).start();
    }

    @Test
    public void client_should_be_able_to_schedule_executions() {
        SchedulerClient client = SchedulerClient.Builder.create(DB.getSchedulerDataSource()).build();
        client.schedule(oneTimeTaskA.instance("1"), settableClock.now());

        scheduler.runAnyDueExecutions();
        assertThat(onetimeTaskHandlerA.timesExecuted, CoreMatchers.is(1));
    }

    @Test
    public void should_be_able_to_schedule_other_executions_from_an_executionhandler() {
        scheduler.schedule(scheduleAnotherTask.instance("1"), settableClock.now());
        scheduler.runAnyDueExecutions();
        assertThat(scheduleAnother.timesExecuted, CoreMatchers.is(1));
        assertThat(onetimeTaskHandlerA.timesExecuted, CoreMatchers.is(0));

        scheduler.tick(ofSeconds(1));
        scheduler.runAnyDueExecutions();
        assertThat(onetimeTaskHandlerA.timesExecuted, CoreMatchers.is(1));
    }

    @Test
    public void client_should_be_able_to_fetch_executions_for_task() {
        SchedulerClient client = SchedulerClient.Builder.create(DB.getSchedulerDataSource(), oneTimeTaskA, oneTimeTaskB).build();
        client.schedule(oneTimeTaskA.instance("1"), settableClock.now());
        client.schedule(oneTimeTaskA.instance("2"), settableClock.now());
        client.schedule(oneTimeTaskB.instance("10"), settableClock.now());
        client.schedule(oneTimeTaskB.instance("11"), settableClock.now());
        client.schedule(oneTimeTaskB.instance("12"), settableClock.now());

        assertThat(countAllExecutions(client), is(5));
        assertThat(countExecutionsForTask(client, oneTimeTaskA.getName(), Void.class), is(2));
        assertThat(countExecutionsForTask(client, oneTimeTaskB.getName(), Void.class), is(3));
    }

    @Test
    public void client_should_be_able_to_fetch_single_scheduled_execution() {
        SchedulerClient client = SchedulerClient.Builder.create(DB.getSchedulerDataSource(), oneTimeTaskA).build();
        client.schedule(oneTimeTaskA.instance("1"), settableClock.now());

        assertThat(client.getScheduledExecution(new ScheduledTasks(oneTimeTaskA.getName(), "1")), not(OptionalMatchers.empty()));
        assertThat(client.getScheduledExecution(new ScheduledTasks(oneTimeTaskA.getName(), "2")), OptionalMatchers.empty());
        assertThat(client.getScheduledExecution(new ScheduledTasks(oneTimeTaskB.getName(), "1")), OptionalMatchers.empty());
    }

    @Test
    public void client_should_be_able_to_reschedule_executions() {
        String data1 = "data1";
        Map<String, Object> data2 =new HashMap<String, Object>();
        data2.put("data2", "data2");

        scheduler.schedule(savingTask.instance("1", data1), settableClock.now().plusSeconds(1));
        scheduler.reschedule(savingTask.instance("1"), settableClock.now());
        scheduler.runAnyDueExecutions();
        assertThat(savingHandler.savedData, CoreMatchers.is(data1));

        scheduler.schedule(savingTask.instance("2", "none"), settableClock.now().plusSeconds(1));
        scheduler.reschedule(savingTask.instance("2"), settableClock.now(), data2);
        scheduler.runAnyDueExecutions();
        assertThat(savingHandler.savedData, CoreMatchers.is(data2));

        scheduler.tick(ofSeconds(1));
        scheduler.runAnyDueExecutions();
        assertThat(savingHandler.savedData, CoreMatchers.is(data2));
    }

    private int countAllExecutions(SchedulerClient client) {
        AtomicInteger counter = new AtomicInteger(0);
        client.getScheduledExecutions((ScheduledExecution<Object> execution) -> {counter.incrementAndGet();});
        return counter.get();
    }

    private <T> int countExecutionsForTask(SchedulerClient client, String taskName, Class<T> dataClass) {
        AtomicInteger counter = new AtomicInteger(0);
        client.getScheduledExecutionsForTask(taskName, dataClass, (ScheduledExecution<T> execution) -> {counter.incrementAndGet();});
        return counter.get();
    }


    public static class ScheduleAnotherTaskHandler implements VoidExecutionHandler {
        public int timesExecuted = 0;
        private final ScheduledTasks secondTask;
        private final Instant instant;

        public ScheduleAnotherTaskHandler(ScheduledTasks secondTask, Instant instant) {
            this.secondTask = secondTask;
            this.instant = instant;
        }

        @Override
        public void execute(ScheduledTasks taskInstance, ExecutionContext executionContext) {
            executionContext.getSchedulerClient().schedule(secondTask, instant);
            this.timesExecuted++;
        }
    }
}
