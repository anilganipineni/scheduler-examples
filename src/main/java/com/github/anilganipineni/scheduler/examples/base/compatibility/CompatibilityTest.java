package com.github.anilganipineni.scheduler.examples.base.compatibility;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.LoggerFactory;

import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.SchedulerBuilder;
import com.github.anilganipineni.scheduler.SchedulerName;
import com.github.anilganipineni.scheduler.StatsRegistry;
import com.github.anilganipineni.scheduler.TaskResolver;
import com.github.anilganipineni.scheduler.dao.DbUtils;
import com.github.anilganipineni.scheduler.dao.JdbcTaskRepository;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.examples.base.ExampleUtils;
import com.github.anilganipineni.scheduler.examples.base.StopSchedulerExtension;
import com.github.anilganipineni.scheduler.examples.base.TestTasks;
import com.github.anilganipineni.scheduler.examples.base.TestTasks.DoNothingHandler;
import com.github.anilganipineni.scheduler.schedule.FixedDelay;
import com.github.anilganipineni.scheduler.task.OneTimeTask;
import com.github.anilganipineni.scheduler.task.RecurringTask;
import com.google.common.collect.Lists;


public abstract class CompatibilityTest {

    private static final int POLLING_LIMIT = 10_000;

    @RegisterExtension
    public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

    private TestTasks.CountingHandler delayingHandlerOneTime;
    private TestTasks.CountingHandler delayingHandlerRecurring;
    private OneTimeTask oneTime;
    private RecurringTask recurring;
    private RecurringTask recurringWithData;
    private TestTasks.SimpleStatsRegistry statsRegistry;
    private Scheduler scheduler;

    public abstract SchedulerDataSource getDataSource();

    @BeforeEach
    public void setUp() {
        delayingHandlerOneTime = new TestTasks.CountingHandler(Duration.ofMillis(200));
        delayingHandlerRecurring = new TestTasks.CountingHandler(Duration.ofMillis(200));

        oneTime = TestTasks.oneTimeWithType("oneTime", delayingHandlerOneTime);
        recurring = TestTasks.recurring("recurring", FixedDelay.of(Duration.ofMillis(10)), delayingHandlerRecurring);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("0", 0);
        recurringWithData = TestTasks.recurringWithData("recurringWithData", map, FixedDelay.of(Duration.ofMillis(10)), new DoNothingHandler());

        statsRegistry = new TestTasks.SimpleStatsRegistry();
        scheduler = SchedulerBuilder.create(getDataSource(), Lists.newArrayList(oneTime, recurring))
                .pollingInterval(Duration.ofMillis(10))
                .heartbeatInterval(Duration.ofMillis(100))
                .statsRegistry(statsRegistry)
                .build();
        stopScheduler.register(scheduler);
    }

    @AfterEach
    public void clearTables() {
        assertTimeout(Duration.ofSeconds(20), () ->
            ExampleUtils.clearTables(getDataSource().rdbmsDataSource())
        );
    }

    @Test
    public void test_compatibility() {
        assertTimeout(Duration.ofSeconds(20), () -> {
            scheduler.start();

            scheduler.schedule(oneTime.instance("id1"), Instant.now());
            scheduler.schedule(oneTime.instance("id1"), Instant.now()); //duplicate
            scheduler.schedule(recurring.instance("id1"), Instant.now());
            scheduler.schedule(recurring.instance("id2"), Instant.now());
            scheduler.schedule(recurring.instance("id3"), Instant.now());
            scheduler.schedule(recurring.instance("id4"), Instant.now());

            sleep(Duration.ofSeconds(10));

            scheduler.stop();
            assertThat(statsRegistry.unexpectedErrors.get(), is(0));
            assertThat(delayingHandlerRecurring.timesExecuted, greaterThan(10));
            assertThat(delayingHandlerOneTime.timesExecuted, is(1));
        });
    }

    @Test
    public void test_jdbc_repository_compatibility() {
        assertTimeout(Duration.ofSeconds(20), () -> {
            doJDBCRepositoryCompatibilityTestUsingData(null);
        });
    }

    @Test
    public void test_jdbc_repository_compatibility_with_data() {
        assertTimeout(Duration.ofSeconds(20), () -> {
            doJDBCRepositoryCompatibilityTestUsingData("my data");
        });
    }

    private void doJDBCRepositoryCompatibilityTestUsingData(String data) {
        TaskResolver taskResolver = new TaskResolver(StatsRegistry.NOOP, new ArrayList<>());
        taskResolver.addTask(oneTime);

        final JdbcTaskRepository jdbcTaskRepository = (JdbcTaskRepository) DbUtils.getRepository(getDataSource(), taskResolver, new SchedulerName.Fixed("scheduler1"));

        final Instant now = Instant.now();

        final ScheduledTasks taskInstance = oneTime.instance("id1", data);
        final ScheduledTasks newExecution = new ScheduledTasks(now, "id1", data);
        jdbcTaskRepository.createIfNotExists(newExecution);
        assertThat((jdbcTaskRepository.getExecution(taskInstance)).get().getExecutionTime(), is(now));

        final List<ScheduledTasks> due = jdbcTaskRepository.getDue(now, POLLING_LIMIT);
        assertThat(due, hasSize(1));
        final Optional<ScheduledTasks> pickedExecution = jdbcTaskRepository.pick(due.get(0), Instant.now());
        assertThat(pickedExecution.isPresent(), is(true));

        assertThat(jdbcTaskRepository.getDue(now, POLLING_LIMIT), hasSize(0));

        jdbcTaskRepository.updateHeartbeat(pickedExecution.get(), now.plusSeconds(1));
        assertThat(jdbcTaskRepository.getDeadExecutions(now.plus(Duration.ofDays(1))), hasSize(1));

        jdbcTaskRepository.reschedule(pickedExecution.get(), now.plusSeconds(1), now.minusSeconds(1), now.minusSeconds(1), 0, null);
        assertThat(jdbcTaskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
        assertThat(jdbcTaskRepository.getDue(now.plus(Duration.ofMinutes(1)), POLLING_LIMIT), hasSize(1));

        final Optional<ScheduledTasks> rescheduled = jdbcTaskRepository.getExecution(taskInstance);
        assertThat(rescheduled.isPresent(), is(true));
        assertThat(rescheduled.get().getLastHeartbeat(), nullValue());
        assertThat(rescheduled.get().isPicked(), is(false));
        assertThat(rescheduled.get().getPickedBy(), nullValue());
        assertThat(rescheduled.get().getTaskData(), is(data));
        jdbcTaskRepository.remove(rescheduled.get());
    }

    @Test
    public void test_jdbc_repository_compatibility_set_data() {
        TaskResolver taskResolver = new TaskResolver(StatsRegistry.NOOP, new ArrayList<>());
        taskResolver.addTask(recurringWithData);

        final JdbcTaskRepository jdbcTaskRepository = (JdbcTaskRepository) DbUtils.getRepository(getDataSource(), taskResolver, new SchedulerName.Fixed("scheduler1"));

        final Instant now = Instant.now();

        final ScheduledTasks taskInstance = recurringWithData.instance("id1", "1");
        final ScheduledTasks newExecution = new ScheduledTasks(now, "id1", "1");

        jdbcTaskRepository.createIfNotExists(newExecution);

        /*ScheduledTasks round1 = jdbcTaskRepository.getExecution(taskInstance).get();
        assertEquals(round1.getTaskData(), 1);
        jdbcTaskRepository.reschedule(round1, now.plusSeconds(1), now.minusSeconds(1), now.minusSeconds(1), 0, 2);*/

        ScheduledTasks round2 = jdbcTaskRepository.getExecution(taskInstance).get();
        assertEquals(round2.getTaskData(), 2);

        jdbcTaskRepository.reschedule(round2, now.plusSeconds(2), now.minusSeconds(2), now.minusSeconds(2), 0, null);
        ScheduledTasks round3 = jdbcTaskRepository.getExecution(taskInstance).get();
        assertNull(round3.getTaskData());
    }


    private void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            LoggerFactory.getLogger(CompatibilityTest.class).info("Interrupted");
        }
    }


}
