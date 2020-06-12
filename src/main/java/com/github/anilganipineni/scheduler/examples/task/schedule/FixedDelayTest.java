package com.github.anilganipineni.scheduler.examples.task.schedule;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

import com.github.anilganipineni.scheduler.schedule.FixedDelay;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FixedDelayTest {
    @Test
    public void equals_and_hash_code() {
        EqualsVerifier.forClass(FixedDelay.class).verify();
    }

    @Test
    public void to_string() {
        assertEquals("FixedDelay duration=PT2M", FixedDelay.of(Duration.ofMinutes(2)).toString());
    }
}
