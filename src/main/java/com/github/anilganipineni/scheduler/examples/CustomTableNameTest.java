package com.github.anilganipineni.scheduler.examples;

import static com.github.anilganipineni.scheduler.dao.rdbms.PreparedStatementSetter.NOOP;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.anilganipineni.scheduler.SchedulerName;
import com.github.anilganipineni.scheduler.StatsRegistry;
import com.github.anilganipineni.scheduler.TaskResolver;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.dao.rdbms.JdbcRunner;
import com.github.anilganipineni.scheduler.dao.rdbms.JdbcTaskRepository;
import com.github.anilganipineni.scheduler.dao.rdbms.ResultSetMapper;
import com.github.anilganipineni.scheduler.task.OneTimeTask;
import com.github.anilganipineni.scheduler.task.Task;

public class CustomTableNameTest {

    private static final String SCHEDULER_NAME = "scheduler1";

    private static final String CUSTOM_TABLENAME = "custom_tablename";

    @RegisterExtension
    public EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();

    private JdbcTaskRepository taskRepository;
    private OneTimeTask oneTimeTask;

    @BeforeEach
    public void setUp() {
        oneTimeTask = TestTasks.oneTime("OneTime", TestTasks.DO_NOTHING);
        List<Task> knownTasks = new ArrayList<Task>();
        knownTasks.add(oneTimeTask);
        taskRepository = new JdbcTaskRepository(DB.getDataSource(), new TaskResolver(StatsRegistry.NOOP, knownTasks), new SchedulerName.Fixed(SCHEDULER_NAME));

        DbUtils.runSqlResource("postgresql_custom_tablename.sql").accept(DB.getDataSource());
    }

    @Test
    public void can_customize_table_name() {
        Instant now = Instant.now();
        taskRepository.createIfNotExists(new ScheduledTasks(now, oneTimeTask.getName(), "id1"));

        JdbcRunner jdbcRunner = new JdbcRunner(DB.getDataSource());
        jdbcRunner.execute("SELECT count(1) AS number_of_tasks FROM " + CUSTOM_TABLENAME, NOOP, (ResultSetMapper<Integer>) rs -> rs.getInt("number_of_tasks"));

    }

    @AfterEach
    public void tearDown() {
        new JdbcRunner(DB.getDataSource()).execute("DROP TABLE " + CUSTOM_TABLENAME, NOOP);
    }

}
