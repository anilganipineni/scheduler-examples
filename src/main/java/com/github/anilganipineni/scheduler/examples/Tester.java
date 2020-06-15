package com.github.anilganipineni.scheduler.examples;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.SchedulerBuilder;
import com.github.anilganipineni.scheduler.dao.CassandraDataSource;
import com.github.anilganipineni.scheduler.dao.DataSourceType;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.schedule.FixedDelay;
import com.github.anilganipineni.scheduler.task.Task;
import com.github.anilganipineni.scheduler.task.TaskFactory;

/**
 * @author akganipineni
 */
public class Tester {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(CassandraDataSourceImpl.class);
	private static final String RDBMS_DRIVER		= "com.mysql.jdbc.Driver";
	private static final String RDBMS_URL			= "jdbc:mysql://localhost:3306/gstwrapper?useSSL=false";
	private static final String RDBMS_USER			= "root";
	private static final String RDBMS_PWD			= "admin";
	
	private static final String CASSANDRA_SERVER	= "127.0.0.1";
	private static final Integer CASSANDRA_PORT		= 9042;
	private static final String CASSANDRA_KYE_SPACE	= "gsp";
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		startScheduler();
	}
	/**
	 * Sample function to start scheduler
	 */
	private static void startScheduler() {
		Task hourlyTask = TaskFactory.recurring("my-hourly-task", FixedDelay.ofHours(6))
		        .execute((task, ctx) -> {
		        	System.out.println(new Date() + " - Anil Hourly Scheduler Executed..........");
		        });
		
		Task minutesTask = TaskFactory.recurring("my-minutes-task", FixedDelay.ofMinutes(5))
		        .execute((task, ctx) -> {
		        	System.out.println(new Date() + " - Anil Minutes Scheduler Executed..........");
		        });
		
		System.out.println(new Date() + " - Enabling the Anil Scheduler..........");
		
		List<Task> startTasks = Arrays.asList(hourlyTask, minutesTask);
		Scheduler s = SchedulerBuilder.create(getDataSource()).threads(5).enableImmediateExecution().startTasks(startTasks).build();

		// Task(s) will automatically scheduled on startup if not already started (i.e. if not exists in the db)
		s.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    @Override
		    public void run() {
				System.out.println(new Date() + " - Received shutdown signal.");
				if(s != null) {
					s.stop();
				}
		    }
		});
		Runnable r = new Runnable() {
			/**
			 * @see java.lang.Runnable#run()
			 */
			@Override
			public void run() {
				System.out.println(new Date() + " - " + Thread.currentThread().getName() + " - Start..........!");
				try {
					Thread.sleep(1 * 60 * 1000);
					System.out.println(new Date() + " - " + Thread.currentThread().getName() + " - After Sleep..........!");

					Task oneTimeTask = TaskFactory.oneTime("my-onetime-task")
			                .execute((task, ctx) -> {
			                	System.out.println(new Date() + " - Anil One Time Scheduler Executed and Custom data Id : " + task.getTaskId());
			                });

					Map<String, Object> map = new HashMap<String, Object>();
					map.put("id", Long.parseLong("1001"));
					s.addTask(oneTimeTask);

						s.schedule(oneTimeTask.instance("1045", map), Instant.now().plusSeconds(600));
					System.out.println(new Date() + " - " + Thread.currentThread().getName() + " - Thread completed and scheduleed the one time task!");
					
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		};
		
		new Thread(r, "New Ontetime Task").start();
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
    /**
     * @author akganipineni
     */
    private static class CassandraDataSourceImpl implements CassandraDataSource {
    	/**
    	 * The singleton session for the whole application
    	 */
    	private Session m_session = null;
    	/**
    	 * The initialization method to set the properties.
    	 * with the @PostConstruct annotation, Spring will call this method after bean construct
    	 */
    	private void createSession() {
    		try {
    			logger.info("Creating Cassandra session. Server : " + CASSANDRA_SERVER + ", port : " + CASSANDRA_PORT + ", KeySpace : " + CASSANDRA_KYE_SPACE);
    			// Cluster cluster = Cluster.builder().addContactPoint(m_server).withPort(m_port).build();
    			SocketOptions options = new SocketOptions().setReadTimeoutMillis(12000);
    			Cluster cluster = Cluster.builder()
    									 .addContactPoints(CASSANDRA_SERVER.split(","))
    									 .withPort(CASSANDRA_PORT)
    									 .withSocketOptions(options)
    									 .build();
    			// Configure our LocalDateCodec
    			CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
    			codecRegistry.register(InstantCodec.instance); // maps Instant to time stamp;
    			// codecRegistry.register(LocalDateCodec.instance); // maps LocalDate to date;
    			// codecRegistry.register(LocalTimeCodec.instance); // maps LocalTime to time.
    			
    			m_session = cluster.connect(CASSANDRA_KYE_SPACE);
    			if(m_session == null) {
    				logger.error("Either Cassandra is not running or Session is not initilised properly!");
    				if(cluster != null) {
    					cluster.close();
    				}
    				 throw new IllegalStateException("Either Cassandra is not running or Session is not initilised properly!");
    			}
    			logger.info("Created Cassandra session successfully.........................");
    			
    		} catch(Exception e) {
    			logger.error("Either Cassandra is not running or Session is not initilised properly!", e);
    			 throw new IllegalStateException("Either Cassandra is not running or Session is not initilised properly!");
    		}
    	}
    	/**
    	 * @see com.CassandraDataSource.gst.core.dao.CassandraDao#getSession()
    	 */
    	@Override
    	public Session getSession() {
    		if(m_session == null || m_session.getCluster() == null) {
    			synchronized (Session.class) {
    				if(m_session == null || m_session.getCluster() == null) {
    					logger.info("**************creating session******************");
    					createSession();
    					logger.info("**************Session is created successfully, Session object" + m_session);
    				}
    			}
    		}
    		
    		return m_session;
    	}
    	/**
    	 * @see com.CassandraDataSource.gst.core.dao.CassandraDao#closeSession()
    	 */
    	@Override
    	public void closeSession() {
    		if(m_session != null) {
    			Cluster cluster = m_session.getCluster();
    			m_session.close();
    			if(cluster != null) {
    				cluster.close();
    			}
    		}
    	}
    }
	/**
	 * Prepares and returns the {@link SchedulerDataSource}
	 * 
	 * @return
	 */
	private static SchedulerDataSource getDataSource() {
		SchedulerDataSource ds = new SchedulerDataSource() {
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
		};
		return ds;
	}
}
