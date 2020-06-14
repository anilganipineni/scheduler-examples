package com.github.anilganipineni.scheduler.examples;

import java.util.Date;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.github.anilganipineni.scheduler.dao.CassandraDataSource;
import com.github.anilganipineni.scheduler.dao.DataSourceType;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;

/**
 * @author akganipineni
 */
public class SchedulerDataSourceImpl implements SchedulerDataSource {
	private static final String RDBMS_DRIVER	= "com.mysql.jdbc.Driver";
	private static final String RDBMS_URL		= "jdbc:mysql://localhost:3306/gstwrapper?useSSL=false";
	private static final String RDBMS_USER		= "root";
	private static final String RDBMS_PWD		= "admin";
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
		return datasource();
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerDataSource#cassandraDataSource()
	 */
	@Override
	public CassandraDataSource cassandraDataSource() {
		return new CassandraDataSourceImpl();
	}
    /**
     * @return
     */
    private static DataSource datasource() {
		System.out.println(new Date() + " - Creating the Data Source for db type : RDBMS");
    	DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName(RDBMS_DRIVER);
        ds.setUrl(RDBMS_URL);
        ds.setUsername(RDBMS_USER);
        ds.setPassword(RDBMS_PWD);
        System.out.println(new Date() + " - DataSource created for " + RDBMS_URL);
        
        return ds;
    }
}
