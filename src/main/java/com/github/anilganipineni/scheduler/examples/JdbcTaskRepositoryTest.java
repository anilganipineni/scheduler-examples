package com.github.anilganipineni.scheduler.examples;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.anilganipineni.scheduler.SchedulerName;
import com.github.anilganipineni.scheduler.StatsRegistry.SchedulerStatsEvent;
import com.github.anilganipineni.scheduler.TaskResolver;
import com.github.anilganipineni.scheduler.dao.JdbcTaskRepository;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.examples.helper.TestableRegistry;
import com.github.anilganipineni.scheduler.task.OneTimeTask;
import com.github.anilganipineni.scheduler.task.Task;


public class JdbcTaskRepositoryTest {

    public static final String SCHEDULER_NAME = "scheduler1";
    private static final int POLLING_LIMIT = 10_000;
    @RegisterExtension
    public EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();

    private JdbcTaskRepository taskRepository;
    private OneTimeTask oneTimeTask;
    private OneTimeTask alternativeOneTimeTask;
    private OneTimeTask oneTimeTaskWithData;
    private TaskResolver taskResolver;
    private TestableRegistry testableRegistry;

    @BeforeEach
    public void setUp() {
        oneTimeTask = TestTasks.oneTime("OneTime", TestTasks.DO_NOTHING);
        alternativeOneTimeTask = TestTasks.oneTime("AlternativeOneTime", TestTasks.DO_NOTHING);
        oneTimeTaskWithData = TestTasks.oneTime("OneTimeWithData", new TestTasks.DoNothingHandler());
        List<Task> knownTasks = new ArrayList<Task>();
        knownTasks.add(oneTimeTask);
        knownTasks.add(oneTimeTaskWithData);
        knownTasks.add(alternativeOneTimeTask);
        testableRegistry = new TestableRegistry(true, Collections.emptyList());
        taskResolver = new TaskResolver(testableRegistry, knownTasks);
        taskRepository = new JdbcTaskRepository(DB.getDataSource(), taskResolver, new SchedulerName.Fixed(SCHEDULER_NAME));
    }

    @Test
    public void test_createIfNotExists() {
        Instant now = Instant.now();
        assertTrue(taskRepository.createIfNotExists(new ScheduledTasks(now, oneTimeTask.getName(), "id1")));
        assertFalse(taskRepository.createIfNotExists(new ScheduledTasks(now, oneTimeTask.getName(), "id1")));
        assertTrue(taskRepository.createIfNotExists(new ScheduledTasks(now, oneTimeTask.getName(), "id2")));
    }

    @Test
    public void get_due_should_only_include_due_executions() {
        Instant now = Instant.now();

        taskRepository.createIfNotExists(new ScheduledTasks(now, oneTimeTask.getName(), "id1"));
        assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(1));
        assertThat(taskRepository.getDue(now.minusSeconds(1), POLLING_LIMIT), hasSize(0));
    }

    @Test
    public void get_due_should_honor_max_results_limit() {
        Instant now = Instant.now();

        taskRepository.createIfNotExists(new ScheduledTasks(now, oneTimeTask.getName(), "id1"));
        taskRepository.createIfNotExists(new ScheduledTasks(now, oneTimeTask.getName(), "id2"));
        assertThat(taskRepository.getDue(now, 1), hasSize(1));
        assertThat(taskRepository.getDue(now, 2), hasSize(2));
    }

    @Test
    public void get_due_should_be_sorted() {
        Instant now = Instant.now();
        IntStream.range(0, 100).forEach(i ->
                        taskRepository.createIfNotExists(new ScheduledTasks(now.minusSeconds(new Random().nextInt(10000)), oneTimeTask.getName(), "id" + i))
        );
        List<ScheduledTasks> due = taskRepository.getDue(now, POLLING_LIMIT);
        assertThat(due, hasSize(100));

        List<ScheduledTasks> sortedDue = new ArrayList<>(due);
        sortedDue.sort(Comparator.comparing(ScheduledTasks::getExecutionTime));
        assertThat(due, is(sortedDue));
    }

    @Test
    public void get_due_should_not_include_previously_unresolved() {
        Instant now = Instant.now();
        final OneTimeTask unresolved1 = TestTasks.oneTime("unresolved1", TestTasks.DO_NOTHING);
        final OneTimeTask unresolved2 = TestTasks.oneTime("unresolved2", TestTasks.DO_NOTHING);
        final OneTimeTask unresolved3 = TestTasks.oneTime("unresolved3", TestTasks.DO_NOTHING);

        assertThat(taskResolver.getUnresolved(), hasSize(0));

        // 1
        taskRepository.createIfNotExists(new ScheduledTasks(now, unresolved1.getName(), "id"));
        assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
        assertThat(taskResolver.getUnresolved(), hasSize(1));
        assertEquals(1, testableRegistry.getCount(SchedulerStatsEvent.UNRESOLVED_TASK));

        assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
        assertThat(taskResolver.getUnresolved(), hasSize(1));
        assertEquals(1, testableRegistry.getCount(SchedulerStatsEvent.UNRESOLVED_TASK),
            "ScheduledTasks should not have have been in the ResultSet");

        // 1, 2
        taskRepository.createIfNotExists(new ScheduledTasks(now, unresolved2.getName(), "id"));
        assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
        assertThat(taskResolver.getUnresolved(), hasSize(2));
        assertEquals(2, testableRegistry.getCount(SchedulerStatsEvent.UNRESOLVED_TASK));

        // 1, 2, 3
        taskRepository.createIfNotExists(new ScheduledTasks(now, unresolved3.getName(), "id"));
        assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
        assertThat(taskResolver.getUnresolved(), hasSize(3));
        assertEquals(3, testableRegistry.getCount(SchedulerStatsEvent.UNRESOLVED_TASK));
    }

    @Test
    public void picked_executions_should_not_be_returned_as_due() {
        Instant now = Instant.now();
        taskRepository.createIfNotExists(new ScheduledTasks(now, oneTimeTask.getName(), "id1"));
        List<ScheduledTasks> due = taskRepository.getDue(now, POLLING_LIMIT);
        assertThat(due, hasSize(1));

        taskRepository.pick(due.get(0), now);
        assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
    }

    @Test
    public void picked_execution_should_have_information_about_which_scheduler_processes_it() {
        Instant now = Instant.now();
        final ScheduledTasks instance = oneTimeTask.instance("id1");
        taskRepository.createIfNotExists(new ScheduledTasks(now, oneTimeTask.getName(), "id1"));
        List<ScheduledTasks> due = taskRepository.getDue(now, POLLING_LIMIT);
        assertThat(due, hasSize(1));
        taskRepository.pick(due.get(0), now);

        final Optional<ScheduledTasks> pickedExecution = taskRepository.getExecution(instance);
        assertThat(pickedExecution.isPresent(), is(true));
        assertThat(pickedExecution.get().picked, is(true));
        assertThat(pickedExecution.get().pickedBy, is(SCHEDULER_NAME));
        assertThat(pickedExecution.get().lastHeartbeat, notNullValue());
        assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
    }

    @Test
    public void should_not_be_able_to_pick_execution_that_has_been_rescheduled() {
        Instant now = Instant.now();
        taskRepository.createIfNotExists(new ScheduledTasks(now, oneTimeTask.getName(), "id1"));

        List<ScheduledTasks> due = taskRepository.getDue(now, POLLING_LIMIT);
        assertThat(due, hasSize(1));
        final ScheduledTasks execution = due.get(0);
        final Optional<ScheduledTasks> pickedExecution = taskRepository.pick(execution, now);
        assertThat(pickedExecution.isPresent(), is(true));
        taskRepository.reschedule(pickedExecution.get(), now.plusSeconds(1), now, null, 0);

        assertThat(taskRepository.pick(pickedExecution.get(), now).isPresent(), is(false));
    }

    @Test
    public void reschedule_should_move_execution_in_time() {
        Instant now = Instant.now();
        final ScheduledTasks instance = oneTimeTask.instance("id1");
        taskRepository.createIfNotExists(new ScheduledTasks(now, oneTimeTask.getName(), "id1"));
        List<ScheduledTasks> due = taskRepository.getDue(now, POLLING_LIMIT);
        assertThat(due, hasSize(1));

        ScheduledTasks execution = due.get(0);
        final Optional<ScheduledTasks> pickedExecution = taskRepository.pick(execution, now);
        final Instant nextExecutionTime = now.plus(Duration.ofMinutes(1));
        taskRepository.reschedule(pickedExecution.get(), nextExecutionTime, now, null, 0);

        assertThat(taskRepository.getDue(now, POLLING_LIMIT), hasSize(0));
        assertThat(taskRepository.getDue(nextExecutionTime, POLLING_LIMIT), hasSize(1));

        final Optional<ScheduledTasks> nextExecution = taskRepository.getExecution(instance);
        assertTrue(nextExecution.isPresent());
        assertThat(nextExecution.get().picked, is(false));
        assertThat(nextExecution.get().pickedBy, nullValue());
        assertThat(nextExecution.get().executionTime, is(nextExecutionTime));
    }

    @Test
    public void reschedule_should_persist_consecutive_failures() {
        Instant now = Instant.now();
        final ScheduledTasks instance = oneTimeTask.instance("id1");
        taskRepository.createIfNotExists(new ScheduledTasks(now, oneTimeTask.getName(), "id1"));
        List<ScheduledTasks> due = taskRepository.getDue(now, POLLING_LIMIT);
        assertThat(due, hasSize(1));

        ScheduledTasks execution = due.get(0);
        final Optional<ScheduledTasks> pickedExecution = taskRepository.pick(execution, now);
        final Instant nextExecutionTime = now.plus(Duration.ofMinutes(1));
        taskRepository.reschedule(pickedExecution.get(), nextExecutionTime, now.minusSeconds(1), now, 1);

        final Optional<ScheduledTasks> nextExecution = taskRepository.getExecution(instance);
        assertTrue(nextExecution.isPresent());
        assertThat(nextExecution.get().consecutiveFailures, is(1));
    }

    @Test
    public void reschedule_should_update_data_if_specified() {
        Instant now = Instant.now();
        final ScheduledTasks instance = oneTimeTaskWithData.instance("id1", 1);
        taskRepository.createIfNotExists(new ScheduledTasks(now, oneTimeTaskWithData.getName(), "id1", 1));

        ScheduledTasks created = taskRepository.getExecution(instance).get();
        assertEquals(created.getTaskData(), 1);

        final Instant nextExecutionTime = now.plus(Duration.ofMinutes(1));
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("2", 2);
        taskRepository.reschedule(created, nextExecutionTime, now, null, 0, map);

        final ScheduledTasks rescheduled = taskRepository.getExecution(instance).get();
        assertEquals(rescheduled.getTaskData(), 2);
    }

    @Test
    public void test_get_failing_executions() {
        Instant now = Instant.now();
        taskRepository.createIfNotExists(new ScheduledTasks(now, oneTimeTask.getName(), "id1"));

        List<ScheduledTasks> due = taskRepository.getDue(now, POLLING_LIMIT);
        assertThat(due, hasSize(1));

        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(0));

        taskRepository.reschedule(getSingleDueExecution(), now, now, null, 0);
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(0));

        taskRepository.reschedule(getSingleDueExecution(), now, null, now, 1);
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(1));
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofMinutes(1)), hasSize(1));
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofDays(1)), hasSize(1));

        taskRepository.reschedule(getSingleDueExecution(), now, now.minus(Duration.ofMinutes(1)), now, 1);
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(1));
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofSeconds(1)), hasSize(1));
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofHours(1)), hasSize(0));
    }

    @Test
    public void get_scheduled_executions() {
        Instant now = Instant.now();
        IntStream.range(0, 100).forEach(i ->
                taskRepository.createIfNotExists(new ScheduledTasks(now.plus(new Random().nextInt(10), ChronoUnit.HOURS), oneTimeTask.getName(), "id" + i))
        );
        List<ScheduledTasks> beforePick = new ArrayList<ScheduledTasks>();
        taskRepository.getScheduledExecutions(beforePick::add);
        assertThat(beforePick, hasSize(100));

        taskRepository.pick(beforePick.get(0), Instant.now());
        List<ScheduledTasks> afterPick = new ArrayList<>();
        taskRepository.getScheduledExecutions(afterPick::add);
        assertThat(afterPick, hasSize(99));
    }

    @Test
    public void get_scheduled_by_task_name() {
        Instant now = Instant.now();
        taskRepository.createIfNotExists(new ScheduledTasks(now.plus(new Random().nextInt(10), ChronoUnit.HOURS), oneTimeTask.getName(),"id" + 1));
        taskRepository.createIfNotExists(new ScheduledTasks(now.plus(new Random().nextInt(10), ChronoUnit.HOURS), oneTimeTask.getName(),"id" + 2));
        taskRepository.createIfNotExists(new ScheduledTasks(now.plus(new Random().nextInt(10), ChronoUnit.HOURS), alternativeOneTimeTask.getName(), "id" + 3));

        List<ScheduledTasks> scheduledByTaskName = new ArrayList<>();
        taskRepository.getScheduledExecutions(oneTimeTask.getName(), scheduledByTaskName::add);
        assertThat(scheduledByTaskName, hasSize(2));

        List<ScheduledTasks> alternativeTasks = new ArrayList<>();
        taskRepository.getScheduledExecutions(alternativeOneTimeTask.getName(), alternativeTasks::add);
        assertThat(alternativeTasks, hasSize(1));

        List<ScheduledTasks> empty = new ArrayList<ScheduledTasks>();
        taskRepository.getScheduledExecutions("non-existing", empty::add);
        assertThat(empty, empty());
    }

    @Test
    public void get_dead_executions_should_not_include_previously_unresolved() {
        Instant now = Instant.now();

        // 1
        final Instant timeDied = now.minus(Duration.ofDays(5));
        createDeadExecution(oneTimeTask.instance("id1"), timeDied);

        TaskResolver taskResolverMissingTask = new TaskResolver(testableRegistry);
        JdbcTaskRepository repoMissingTask = new JdbcTaskRepository(DB.getDataSource(), taskResolverMissingTask, new SchedulerName.Fixed(SCHEDULER_NAME));

        assertThat(taskResolverMissingTask.getUnresolved(), hasSize(0));
        assertEquals(0, testableRegistry.getCount(SchedulerStatsEvent.UNRESOLVED_TASK));

        assertThat(repoMissingTask.getDeadExecutions(timeDied), hasSize(0));
        assertThat(taskResolverMissingTask.getUnresolved(), hasSize(1));
        assertEquals(1, testableRegistry.getCount(SchedulerStatsEvent.UNRESOLVED_TASK));

        assertThat(repoMissingTask.getDeadExecutions(timeDied), hasSize(0));
        assertThat(taskResolverMissingTask.getUnresolved(), hasSize(1));
        assertEquals(1, testableRegistry.getCount(SchedulerStatsEvent.UNRESOLVED_TASK));
    }

    private void createDeadExecution(ScheduledTasks taskInstance, Instant timeDied) {
        taskRepository.createIfNotExists(new ScheduledTasks(timeDied, taskInstance.getTaskName(), taskInstance.getId()));
        final ScheduledTasks due = getSingleExecution();

        final Optional<ScheduledTasks> picked = taskRepository.pick(due, timeDied);
        taskRepository.updateHeartbeat(picked.get(), timeDied);
    }

    private ScheduledTasks getSingleDueExecution() {
        List<ScheduledTasks> due = taskRepository.getDue(Instant.now(), POLLING_LIMIT);
        return due.get(0);
    }

    private ScheduledTasks getSingleExecution() {
        List<ScheduledTasks> executions = new ArrayList<>();
        taskRepository.getScheduledExecutions(executions::add);
        return executions.get(0);
    }
}
