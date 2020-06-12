package com.github.anilganipineni.scheduler.examples.compatibility;

import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.examples.EmbeddedPostgresqlExtension;

public class PostgresqlCompatibilityTest extends CompatibilityTest {

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

    @Override
    public SchedulerDataSource getDataSource() {
        return postgres.getSchedulerDataSource();
    }

}
