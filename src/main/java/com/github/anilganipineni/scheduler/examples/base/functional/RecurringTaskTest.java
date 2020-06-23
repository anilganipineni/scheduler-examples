package com.github.anilganipineni.scheduler.examples.base.functional;

import static co.unruly.matchers.OptionalMatchers.contains;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.anilganipineni.scheduler.ManualScheduler;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.examples.base.EmbeddedPostgresqlExtension;
import com.github.anilganipineni.scheduler.examples.base.TestTasks;
import com.github.anilganipineni.scheduler.examples.base.helper.SettableClock;
import com.github.anilganipineni.scheduler.examples.base.helper.TestHelper;
import com.github.anilganipineni.scheduler.exception.SchedulerException;
import com.github.anilganipineni.scheduler.schedule.ScheduleFactory;
import com.github.anilganipineni.scheduler.task.RecurringTask;
import com.github.anilganipineni.scheduler.task.TaskFactory;

public class RecurringTaskTest {

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
    public void should_have_starttime_according_to_schedule_by_default() throws SchedulerException {

        RecurringTask recurringTask = TaskFactory.recurring("recurring-a", ScheduleFactory.daily(LocalTime.of(23, 59))).execute(TestTasks.DO_NOTHING);

        ManualScheduler scheduler = TestHelper.createManualScheduler(postgres.getSchedulerDataSource())
                .clock(clock)
                .startTasks(Arrays.asList(recurringTask))
                .build();

        scheduler.start();

        Optional<ScheduledTasks> firstExecution = scheduler.getScheduledExecution(new ScheduledTasks("recurring-a", RecurringTask.INSTANCE));
        assertThat(firstExecution.map(ScheduledTasks::getExecutionTime),
                contains(ZonedDateTime.of(DATE, LocalTime.of(23, 59), ZONE).toInstant()));
    }

    @Test
    public void should_have_starttime_now_if_overridden_by_schedule() throws SchedulerException {

        RecurringTask recurringTask = TaskFactory.recurring("recurring-a", ScheduleFactory.fixedDelay(Duration.ofHours(1)))
                .execute(TestTasks.DO_NOTHING);

        ManualScheduler scheduler = TestHelper.createManualScheduler(postgres.getSchedulerDataSource())
                .clock(clock)
                .startTasks(Arrays.asList(recurringTask))
                .build();
        scheduler.start();

        Optional<ScheduledTasks> firstExecution = scheduler.getScheduledExecution(new ScheduledTasks("recurring-a", RecurringTask.INSTANCE));

        assertThat(firstExecution.map(ScheduledTasks::getExecutionTime),
                contains(ZonedDateTime.of(DATE, TIME, ZONE).toInstant()));
    }

}
