package com.github.anilganipineni.scheduler.examples.base;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.anilganipineni.scheduler.ExecutionContext;
import com.github.anilganipineni.scheduler.ExecutionOperations;
import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.SchedulerName;
import com.github.anilganipineni.scheduler.StatsRegistry;
import com.github.anilganipineni.scheduler.TaskResolver;
import com.github.anilganipineni.scheduler.Waiter;
import com.github.anilganipineni.scheduler.dao.JdbcTaskRepository;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.examples.base.helper.SettableClock;
import com.github.anilganipineni.scheduler.task.OneTimeTask;
import com.github.anilganipineni.scheduler.task.Task;
import com.github.anilganipineni.scheduler.task.handler.CompletionHandler;
import com.github.anilganipineni.scheduler.task.handler.DeadExecutionHandler;
import com.github.anilganipineni.scheduler.task.handler.ReviveDeadExecution;
import com.github.anilganipineni.scheduler.task.handler.VoidExecutionHandler;
import com.google.common.util.concurrent.MoreExecutors;


public class DeadExecutionsTest {

    private static final int POLLING_LIMIT = 10_000;

    @RegisterExtension
    public EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();

    private Scheduler scheduler;
    private SettableClock settableClock;
    private OneTimeTask oneTimeTask;
    private JdbcTaskRepository jdbcTaskRepository;
    private NonCompletingTask nonCompleting;
    private TestTasks.CountingHandler nonCompletingExecutionHandler;
    private ReviveDead deadExecutionHandler;

    @BeforeEach
    public void setUp() {
        settableClock = new SettableClock();
        oneTimeTask = TestTasks.oneTime("OneTime", TestTasks.DO_NOTHING);
        nonCompletingExecutionHandler = new TestTasks.CountingHandler();
        deadExecutionHandler = new ReviveDead();
        nonCompleting = new NonCompletingTask("NonCompleting", nonCompletingExecutionHandler, deadExecutionHandler);

        TaskResolver taskResolver = new TaskResolver(StatsRegistry.NOOP, oneTimeTask, nonCompleting);

        jdbcTaskRepository = new JdbcTaskRepository(DB.getDataSource(), taskResolver, new SchedulerName.Fixed("scheduler1"));

        scheduler = new Scheduler(settableClock,
                jdbcTaskRepository,
                taskResolver,
                1,
                MoreExecutors.newDirectExecutorService(),
                new SchedulerName.Fixed("test-scheduler"),
                new Waiter(Duration.ZERO),
                Duration.ofMinutes(1),
                false,
                StatsRegistry.NOOP,
                POLLING_LIMIT,
                Duration.ofDays(14),
                new ArrayList<>());

    }

    @Test
    public void scheduler_should_handle_dead_executions() {
        final Instant now = settableClock.now();

        final ScheduledTasks taskInstance = oneTimeTask.instance("id1");
        final ScheduledTasks execution1 = new ScheduledTasks(now.minus(Duration.ofDays(1)), oneTimeTask.getName(), "id1");
        jdbcTaskRepository.createIfNotExists(execution1);

        final List<ScheduledTasks> due = jdbcTaskRepository.getDue(now, POLLING_LIMIT);
        assertThat(due, Matchers.hasSize(1));
        final ScheduledTasks execution = due.get(0);
        final Optional<ScheduledTasks> pickedExecution = jdbcTaskRepository.pick(execution, now);
        jdbcTaskRepository.updateHeartbeat(pickedExecution.get(), now.minus(Duration.ofHours(1)));

        scheduler.detectDeadExecutions();

        final Optional<ScheduledTasks> rescheduled = jdbcTaskRepository.getExecution(taskInstance);
        assertTrue(rescheduled.isPresent());
        assertThat(rescheduled.get().isPicked(), is(false));
        assertThat(rescheduled.get().getPickedBy(), nullValue());

        assertThat(jdbcTaskRepository.getDue(Instant.now(), POLLING_LIMIT), hasSize(1));
    }

    @Test
    public void scheduler_should_detect_dead_execution_that_never_updated_heartbeat() {
        final Instant now = Instant.now();
        settableClock.set(now.minus(Duration.ofHours(1)));
        final Instant oneHourAgo = settableClock.now();

        final ScheduledTasks execution1 = new ScheduledTasks(oneHourAgo, nonCompleting.getName(), "id1");
        jdbcTaskRepository.createIfNotExists(execution1);

        scheduler.executeDue();
        assertThat(nonCompletingExecutionHandler.timesExecuted, is(1));

        scheduler.executeDue();
        assertThat(nonCompletingExecutionHandler.timesExecuted, is(1));

        settableClock.set(Instant.now());

        scheduler.detectDeadExecutions();
        assertThat(deadExecutionHandler.timesCalled, is(1));

        settableClock.set(Instant.now());

        scheduler.executeDue();
        assertThat(nonCompletingExecutionHandler.timesExecuted, is(2));
    }

    public static class NonCompletingTask extends Task {
        private final VoidExecutionHandler handler;

        public NonCompletingTask(String name, VoidExecutionHandler handler, DeadExecutionHandler deadExecutionHandler) {
            super(name, (executionComplete, executionOperations) -> {}, deadExecutionHandler);
            this.handler = handler;
        }

        @Override
        public CompletionHandler execute(ScheduledTasks taskInstance, ExecutionContext executionContext) {
            handler.execute(taskInstance, executionContext);
            throw new RuntimeException("simulated unexpected exception");
        }
    }

    public static class ReviveDead extends ReviveDeadExecution {
        public int timesCalled = 0;

        @Override
        public void deadExecution(ScheduledTasks execution, ExecutionOperations executionOperations) {
            timesCalled++;
            super.deadExecution(execution, executionOperations);
        }
    }

}
