package com.github.anilganipineni.scheduler.examples.helper;

import java.util.concurrent.CountDownLatch;

import com.github.anilganipineni.scheduler.StatsRegistry;

public class RanExecuteDueCondition implements TestableRegistry.Condition {

    private final CountDownLatch count;

    public RanExecuteDueCondition(int waitForCount) {
        count = new CountDownLatch(waitForCount);
    }

    @Override
    public void waitFor() {
        try {
            count.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void apply(StatsRegistry.SchedulerStatsEvent e) {
        if (e == StatsRegistry.SchedulerStatsEvent.RAN_EXECUTE_DUE) {
            count.countDown();
        }
    }

    @Override
    public void apply(StatsRegistry.CandidateStatsEvent e) {
    }

    @Override
    public void apply(StatsRegistry.ExecutionStatsEvent e) {
    }
}
