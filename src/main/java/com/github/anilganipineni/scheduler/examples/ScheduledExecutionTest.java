package com.github.anilganipineni.scheduler.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.github.anilganipineni.scheduler.ScheduledExecution;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.exception.SchedulerException;

public class ScheduledExecutionTest {

    @Test
    public void test_equals() {
        Instant now = Instant.now();
        assertEquals(createExecution("task", "1", now), createExecution("task", "1", now));
        assertNotEquals(createExecution("task", "1", now), createExecution("task2", "1", now));
        assertNotEquals(createExecution("task", "1", now), createExecution("task", "2", now));
        assertNotEquals(createExecution("task", "1", now), createExecution("task", "1", now.plusSeconds(1)));
    }

    private ScheduledExecution<Void> createExecution(String taskname, String id, Instant executionTime) {
        return new ScheduledExecution<Void>(Void.class, new ScheduledTasks(executionTime, taskname, id));
    }

    @Test
    public void test_data_class_type_equals() {
        Instant now = Instant.now();
        ScheduledTasks execution = new ScheduledTasks(now, "id1", "1");

        ScheduledExecution<Integer> scheduledExecution = new ScheduledExecution<>(Integer.class, execution);
        try {
			assertEquals(new Integer(1), scheduledExecution.getData());
		} catch (SchedulerException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}
    }

    @Test
    public void test_data_class_type_not_equals() {
        assertThrows(ScheduledExecution.DataClassMismatchException.class, () -> {

            Instant now = Instant.now();
            ScheduledTasks execution = new ScheduledTasks(now, "id1", "1");

            new ScheduledExecution<>(String.class, execution).getData(); // Instantiate with incorrect type
        });
    }
}
