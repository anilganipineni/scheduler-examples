package com.github.anilganipineni.scheduler.examples.compatibility;

import org.junit.jupiter.api.Disabled;

import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;

@Disabled
public class MssqlCompatibilityTest extends CompatibilityTest {

    public static final String JDBC_URL = "dummy";
    public static final String JDBC_USER = "dummy";
    public static final String JDBC_PASSWORD = "dummy";

    @Override
    public SchedulerDataSource getDataSource() {
        /*final DriverDataSource datasource = new DriverDataSource(JDBC_URL, "com.microsoft.sqlserver.jdbc.SQLServerDriver", new Properties(), JDBC_USER, JDBC_PASSWORD);
        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDataSource(datasource);
        return new HikariDataSource(hikariConfig);*/
    	return null;
    }

}
