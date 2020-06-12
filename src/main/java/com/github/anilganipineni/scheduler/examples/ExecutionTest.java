package com.github.anilganipineni.scheduler.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.task.OneTimeTask;
import com.github.anilganipineni.scheduler.task.RecurringTask;
import com.github.anilganipineni.scheduler.task.schedule.FixedDelay;


public class ExecutionTest {

    @Test
    public void test_equals() {
        Instant now = Instant.now();
        OneTimeTask task = TestTasks.oneTime("OneTime", (instance, executionContext) -> {});
        RecurringTask task2 = TestTasks.recurring("Recurring", FixedDelay.of(Duration.ofHours(1)), TestTasks.DO_NOTHING);

        assertEquals(new ScheduledTasks(now, task.getName(), "id1"), new ScheduledTasks(now, task.getName(), "id1"));
        assertNotEquals(new ScheduledTasks(now, task.getName(), "id1"), new ScheduledTasks(now.plus(Duration.ofMinutes(1)), task.getName(), "id1"));
        assertNotEquals(new ScheduledTasks(now, task.getName(), "id1"), new ScheduledTasks(now, task.getName(), "id2"));

        assertEquals(new ScheduledTasks(now, task2.getName(), "id1"), new ScheduledTasks(now, task2.getName(), "id1"));
        assertNotEquals(new ScheduledTasks(now, task2.getName(), "id1"), new ScheduledTasks(now.plus(Duration.ofMinutes(1)), task2.getName(), "id1"));
        assertNotEquals(new ScheduledTasks(now, task2.getName(), "id1"), new ScheduledTasks(now, task2.getName(), "id2"));

        assertNotEquals(new ScheduledTasks(now, task.getName(), "id1"), new ScheduledTasks(now, task2.getName(), "id1"));
    }
}
