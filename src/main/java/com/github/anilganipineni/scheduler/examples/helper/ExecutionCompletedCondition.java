package com.github.anilganipineni.scheduler.examples.helper;

import java.util.concurrent.CountDownLatch;

import com.github.anilganipineni.scheduler.stats.StatsRegistry;

public class ExecutionCompletedCondition implements TestableRegistry.Condition {

    private final CountDownLatch completed;

    public ExecutionCompletedCondition(int numberCompleted) {
        completed = new CountDownLatch(numberCompleted);
    }

    @Override
    public void waitFor() {
        try {
            completed.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void apply(StatsRegistry.SchedulerStatsEvent e) {
    }

    @Override
    public void apply(StatsRegistry.CandidateStatsEvent e) {
    }

    @Override
    public void apply(StatsRegistry.ExecutionStatsEvent e) {
        if (e == StatsRegistry.ExecutionStatsEvent.COMPLETED) {
            completed.countDown();
        }
    }
}
