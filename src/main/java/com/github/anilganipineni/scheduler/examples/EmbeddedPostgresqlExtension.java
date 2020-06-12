package com.github.anilganipineni.scheduler.examples;

import static com.github.anilganipineni.scheduler.dao.rdbms.PreparedStatementSetter.NOOP;

import java.io.IOException;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import com.github.anilganipineni.scheduler.dao.CassandraDataSource;
import com.github.anilganipineni.scheduler.dao.DataSourceType;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.dao.rdbms.JdbcRunner;
import com.github.anilganipineni.scheduler.dao.rdbms.Mappers;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class EmbeddedPostgresqlExtension implements AfterEachCallback {

    private static EmbeddedPostgres embeddedPostgresql;
    private static SchedulerDataSource schedulerDataSource;
    private static DataSource dataSource;
    private final Consumer<DataSource> initializeSchema;
    private final Consumer<DataSource> cleanupAfter;

    public EmbeddedPostgresqlExtension() {
        this(DbUtils.runSqlResource("/postgresql_tables.sql"), DbUtils::clearTables);
    }

    public EmbeddedPostgresqlExtension(Consumer<DataSource> initializeSchema, Consumer<DataSource> cleanupAfter) {
        this.initializeSchema = initializeSchema;
        this.cleanupAfter = cleanupAfter;
        try {
            if (embeddedPostgresql == null) {
                embeddedPostgresql = initPostgres();

                HikariConfig config = new HikariConfig();
                config.setDataSource(embeddedPostgresql.getDatabase("test", "test"));

                dataSource = new HikariDataSource(config);
                schedulerDataSource = new EmbededSchedulerDataSource(dataSource);

                this.initializeSchema.accept(dataSource);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public DataSource getDataSource() {
        return dataSource;
    }
    /**
	 * @return the schedulerDataSource
	 */
	public SchedulerDataSource getSchedulerDataSource() {
		return schedulerDataSource;
	}

	private EmbeddedPostgres initPostgres() throws IOException {
        final EmbeddedPostgres newEmbeddedPostgresql = EmbeddedPostgres.builder().start();

        final JdbcRunner postgresJdbc = new JdbcRunner(newEmbeddedPostgresql.getPostgresDatabase());

        final Boolean databaseExists = postgresJdbc.execute("SELECT 1 FROM pg_database WHERE datname = 'test'", NOOP, Mappers.NON_EMPTY_RESULTSET);
        if (!databaseExists) {
            postgresJdbc.execute("CREATE DATABASE test", NOOP);
        }

        final Boolean userExists = postgresJdbc.execute("SELECT 1 FROM pg_catalog.pg_user WHERE usename = 'test'", NOOP, Mappers.NON_EMPTY_RESULTSET);
        if (!userExists) {
            postgresJdbc.execute("CREATE ROLE test LOGIN PASSWORD ''", NOOP);
        }

        postgresJdbc.execute("CREATE SCHEMA IF NOT EXISTS AUTHORIZATION test ", NOOP);

        return newEmbeddedPostgresql;
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        cleanupAfter.accept(getDataSource());

    }
    /**
     * @author akganipineni
     */
    public static class EmbededSchedulerDataSource implements SchedulerDataSource {
        /**
         * 
         */
        private final DataSource dataSource;
    	/**
    	 * @param dataSource
    	 */
    	public EmbededSchedulerDataSource(DataSource dataSource) {
    		this.dataSource = dataSource;
    	}
    	/**
    	 * @see com.github.anilganipineni.scheduler.dao.SchedulerDataSource#dataSourceType()
    	 */
    	@Override
    	public DataSourceType dataSourceType() {
    		return DataSourceType.RDBMS;
    	}
    	/**
    	 * @see com.github.anilganipineni.scheduler.dao.SchedulerDataSource#rdbmsDataSource()
    	 */
    	@Override
    	public DataSource rdbmsDataSource() {
    		return dataSource;
    	}
    	/**
    	 * @see com.github.anilganipineni.scheduler.dao.SchedulerDataSource#cassandraDataSource()
    	 */
    	@Override
    	public CassandraDataSource cassandraDataSource() {
    		return null;
    	}    	
    }
}
