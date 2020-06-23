package com.github.anilganipineni.scheduler.examples.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.github.anilganipineni.scheduler.ScheduledExecution;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.exception.DataClassMismatchException;

public class ScheduledExecutionTest {

    @Test
    public void test_equals() {
        Instant now = Instant.now();
        assertEquals(createExecution("task", "1", now), createExecution("task", "1", now));
        assertNotEquals(createExecution("task", "1", now), createExecution("task2", "1", now));
        assertNotEquals(createExecution("task", "1", now), createExecution("task", "2", now));
        assertNotEquals(createExecution("task", "1", now), createExecution("task", "1", now.plusSeconds(1)));
    }

    private ScheduledTasks createExecution(String taskname, String id, Instant executionTime) {
        return new ScheduledTasks(executionTime, taskname, id);
    }

    @Test
    public void test_data_class_type_equals() {
        Instant now = Instant.now();
        ScheduledTasks execution = new ScheduledTasks(now, "id1", "1");

        ScheduledExecution scheduledExecution = new ScheduledExecution(execution);
		assertEquals(new Integer(1), scheduledExecution.getData());
    }

    @Test
    public void test_data_class_type_not_equals() {
        assertThrows(DataClassMismatchException.class, () -> {

            Instant now = Instant.now();
            ScheduledTasks execution = new ScheduledTasks(now, "id1", "1");

            new ScheduledExecution(execution).getData(); // Instantiate with incorrect type
        });
    }
}
