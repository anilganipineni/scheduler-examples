package com.github.anilganipineni.scheduler.examples;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.LoggerFactory;

import com.github.anilganipineni.scheduler.StatsRegistry;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.task.OneTimeTask;
import com.github.anilganipineni.scheduler.task.RecurringTask;
import com.github.anilganipineni.scheduler.task.handler.CompletionHandler;
import com.github.anilganipineni.scheduler.task.handler.FailureHandler;
import com.github.anilganipineni.scheduler.task.handler.OnCompleteRemove;
import com.github.anilganipineni.scheduler.task.handler.VoidExecutionHandler;
import com.github.anilganipineni.scheduler.task.helper.ExecutionComplete;
import com.github.anilganipineni.scheduler.task.helper.ExecutionContext;
import com.github.anilganipineni.scheduler.task.helper.ExecutionOperations;
import com.github.anilganipineni.scheduler.task.schedule.FixedDelay;

public class TestTasks {

    public static final CompletionHandler REMOVE_ON_COMPLETE = new OnCompleteRemove();
    public static final VoidExecutionHandler DO_NOTHING = (taskInstance, executionContext) -> {};

    public static OneTimeTask oneTime(String name, VoidExecutionHandler handler) {
        return new OneTimeTask(name) {
            /**
			 * @see com.github.anilganipineni.scheduler.task.OneTimeTask#executeActual(com.github.anilganipineni.scheduler.dao.ScheduledTasks,
			 *      com.github.anilganipineni.scheduler.task.helper.ExecutionContext)
			 */
			@Override
			public void executeActual(ScheduledTasks task, ExecutionContext context) {
                handler.execute(task, context);
			}
        };
    }

    public static OneTimeTask oneTimeWithType(String name, VoidExecutionHandler handler) {
        return new OneTimeTask(name) {
            @Override
            public void executeActual(ScheduledTasks taskInstance, ExecutionContext executionContext) {
                handler.execute(taskInstance, executionContext);
            }
        };
    }

    public static RecurringTask recurring(String name, FixedDelay schedule, VoidExecutionHandler handler) {
        return new RecurringTask(name, schedule) {
            /**
			 * @see com.github.anilganipineni.scheduler.task.RecurringTask#executeActual(com.github.anilganipineni.scheduler.dao.ScheduledTasks,
			 *      com.github.anilganipineni.scheduler.task.helper.ExecutionContext)
			 */
			@Override
			public void executeActual(ScheduledTasks task, ExecutionContext context) {
                handler.execute(task, context);
			}
        };
    }

    public static  RecurringTask recurringWithData(String name, Object initialData, FixedDelay schedule, VoidExecutionHandler handler) {
        return new RecurringTask(name, schedule, initialData) {
            /**
			 * @see com.github.anilganipineni.scheduler.task.RecurringTask#executeActual(com.github.anilganipineni.scheduler.dao.ScheduledTasks,
			 *      com.github.anilganipineni.scheduler.task.helper.ExecutionContext)
			 */
			@Override
			public void executeActual(ScheduledTasks task, ExecutionContext context) {
                handler.execute(task, context);
			}
        };
    }

    public static class ResultRegisteringCompletionHandler implements CompletionHandler {
        final CountDownLatch waitForNotify = new CountDownLatch(1);
        ExecutionComplete.Result result;
        Optional<Throwable> cause;

        @Override
        public void complete(ExecutionComplete executionComplete, ExecutionOperations executionOperations) {
            this.result = executionComplete.getResult();
            this.cause = executionComplete.getCause();
            executionOperations.stop();
            waitForNotify.countDown();
        }
    }

    public static class ResultRegisteringFailureHandler implements FailureHandler {
        final CountDownLatch waitForNotify = new CountDownLatch(1);
        ExecutionComplete.Result result;
        Optional<Throwable> cause;

        @Override
        public void onFailure(ExecutionComplete executionComplete, ExecutionOperations executionOperations) {
            this.result = executionComplete.getResult();
            this.cause = executionComplete.getCause();
            executionOperations.stop();
            waitForNotify.countDown();
        }
    }

    public static class CountingHandler implements VoidExecutionHandler {
        private final Duration wait;
        public int timesExecuted = 0;

        public CountingHandler() {
            wait = Duration.ofMillis(0);
        }
        public CountingHandler(Duration wait) {
            this.wait = wait;
        }

        @Override
        public void execute(ScheduledTasks taskInstance, ExecutionContext executionContext) {
            this.timesExecuted++;
            try {
                Thread.sleep(wait.toMillis());
            } catch (InterruptedException e) {
                LoggerFactory.getLogger(CountingHandler.class).info("Interrupted.");
            }
        }
    }

    public static class WaitingHandler implements VoidExecutionHandler {

        public final CountDownLatch waitForNotify;

        public WaitingHandler() {
            waitForNotify = new CountDownLatch(1);
        }

        @Override
        public void execute(ScheduledTasks taskInstance, ExecutionContext executionContext) {
            try {
                waitForNotify.await();
            } catch (InterruptedException e) {
                LoggerFactory.getLogger(WaitingHandler.class).info("Interrupted.");
            }
        }
    }

    public static class PausingHandler implements VoidExecutionHandler {

        public final CountDownLatch waitInExecuteUntil;
        public final CountDownLatch waitForExecute;

        public PausingHandler() {
            waitForExecute = new CountDownLatch(1);
            waitInExecuteUntil = new CountDownLatch(1);
        }

        @Override
        public void execute(ScheduledTasks taskInstance, ExecutionContext executionContext) {
            try {
                waitForExecute.countDown();
                waitInExecuteUntil.await();
            } catch (InterruptedException e) {
                LoggerFactory.getLogger(WaitingHandler.class).info("Interrupted.");
            }
        }
    }

    public static class SleepingHandler implements VoidExecutionHandler {

        private final int millis;

        public SleepingHandler(int millis) {
            this.millis = millis;
        }

        @Override
        public void execute(ScheduledTasks taskInstance, ExecutionContext executionContext) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                LoggerFactory.getLogger(WaitingHandler.class).info("Interrupted.");
            }
        }
    }

    public static class DoNothingHandler implements VoidExecutionHandler {

        @Override
        public void execute(ScheduledTasks taskInstance, ExecutionContext executionContext) {
        }
    }

    public static class SavingHandler implements VoidExecutionHandler {
        public Object savedData;

        @Override
        public void execute(ScheduledTasks taskInstance, ExecutionContext executionContext) {
            savedData = taskInstance.getTaskData();
        }
    }

    public static class SimpleStatsRegistry extends StatsRegistry.DefaultStatsRegistry {
        public final AtomicInteger unexpectedErrors = new AtomicInteger(0);

        @Override
        public void register(SchedulerStatsEvent e) {
            if (e == SchedulerStatsEvent.UNEXPECTED_ERROR) {
                unexpectedErrors.incrementAndGet();
            }
            super.register(e);
        }
    }

}
