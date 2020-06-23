package com.github.anilganipineni.scheduler.examples.base.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.anilganipineni.scheduler.ManualScheduler;
import com.github.anilganipineni.scheduler.StatsRegistry;
import com.github.anilganipineni.scheduler.examples.base.EmbeddedPostgresqlExtension;
import com.github.anilganipineni.scheduler.examples.base.ExampleUtils;
import com.github.anilganipineni.scheduler.examples.base.TestTasks;
import com.github.anilganipineni.scheduler.examples.base.helper.SettableClock;
import com.github.anilganipineni.scheduler.examples.base.helper.TestHelper;
import com.github.anilganipineni.scheduler.examples.base.helper.TestableRegistry;
import com.github.anilganipineni.scheduler.task.OneTimeTask;
import com.github.anilganipineni.scheduler.task.TaskFactory;

public class DeleteUnresolvedTest {

    public static final ZoneId ZONE = ZoneId.systemDefault();
    private static final LocalDate DATE = LocalDate.of(2018, 3, 1);
    private static final LocalTime TIME = LocalTime.of(8, 0);
    private SettableClock clock;

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();


    @BeforeEach
    public void setUp() {
        clock = new SettableClock();
        clock.set(ZonedDateTime.of(DATE, TIME, ZONE).toInstant());
    }

    @Test
    public void should_delete_executions_with_old_unresolved_tasknames() {

        OneTimeTask onetime = TaskFactory.oneTime("onetime").execute(TestTasks.DO_NOTHING);


        TestableRegistry testableRegistry = new TestableRegistry(false, Collections.emptyList());
        // Missing task with name 'onetime'
        ManualScheduler scheduler = TestHelper.createManualScheduler(postgres.getSchedulerDataSource())
                .clock(clock)
                .statsRegistry(testableRegistry)
                .build();

        scheduler.schedule(onetime.instance("id1"), clock.now());
        assertEquals(0, testableRegistry.getCount(StatsRegistry.SchedulerStatsEvent.UNRESOLVED_TASK));

        scheduler.runAnyDueExecutions();
        assertEquals(1, testableRegistry.getCount(StatsRegistry.SchedulerStatsEvent.UNRESOLVED_TASK));

        assertEquals(1, ExampleUtils.countExecutions(postgres.getDataSource()));

        scheduler.runDeadExecutionDetection();
        assertEquals(1, ExampleUtils.countExecutions(postgres.getDataSource()));

        clock.set(clock.now().plus(Duration.ofDays(30)));
        scheduler.runDeadExecutionDetection();
        assertEquals(0, ExampleUtils.countExecutions(postgres.getDataSource()));

        scheduler.runDeadExecutionDetection();
    }

}
