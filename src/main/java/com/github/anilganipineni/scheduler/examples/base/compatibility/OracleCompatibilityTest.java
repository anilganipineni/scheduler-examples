package com.github.anilganipineni.scheduler.examples.base.compatibility;

import org.junit.jupiter.api.Disabled;

import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;

@Disabled
public class OracleCompatibilityTest extends CompatibilityTest {

    public static final String JDBC_URL = "dummy";
    public static final String JDBC_USER = "dummy";
    public static final String JDBC_PASSWORD = "dummy";

    @Override
    public SchedulerDataSource getDataSource() {
        /*final DriverDataSource datasource = new DriverDataSource(JDBC_URL, "oracle.jdbc.OracleDriver", new Properties(), JDBC_USER, JDBC_PASSWORD);
        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDataSource(datasource);
        return new HikariDataSource(hikariConfig);*/
    	
    	return null;
    }

}
