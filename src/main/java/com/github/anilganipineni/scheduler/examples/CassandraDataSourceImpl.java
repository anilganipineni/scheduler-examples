package com.github.anilganipineni.scheduler.examples;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.github.anilganipineni.scheduler.dao.CassandraDataSource;

/**
 * @author akganipineni
 */
public class CassandraDataSourceImpl implements CassandraDataSource {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static Logger logger = LogManager.getLogger(CassandraDataSourceImpl.class);
	/**
	 * The <code>readtimeout</code> which should be more than read_request_timeout_in_ms specified
	 * in cassandra.yaml. The default is of 12 seconds so as to be slightly bigger that the 
	 * default Cassandra timeout
	 * 
	 * GSP PROD Setting -- read_request_timeout_in_ms: 60000
	 */
	private int m_readtimeout = 12000;
	/**
	 * The Cassandra Server
	 */
	private String m_server = "127.0.0.1";
	/**
	 * The Cassandra port
	 */
	private Integer m_port = 9042;
	/**
	 * The key space(Schema)
	 */
	private String m_keySpace = "gsp";
	/**
	 * The singleton session for the whole application
	 */
	private Session m_session = null;
	/**
	 * Default Constructor
	 */
	public CassandraDataSourceImpl() {
		/* NO-OP */
	}
	/**
	 * @param server
	 * @param port
	 * @param keySpace
	 */
	public CassandraDataSourceImpl(String server, Integer port, String keySpace) {
		m_server = server;
		m_port = port;
		m_keySpace = keySpace;
	}
	/**
	 * The initialization method to set the properties.
	 * with the @PostConstruct annotation, Spring will call this method after bean construct
	 */
	private void createSession() {
		try {
			logger.info("Creating Cassandra session. Server : " + m_server + ", port : " + m_port + ", KeySpace : " + m_keySpace);
			// Cluster cluster = Cluster.builder().addContactPoint(m_server).withPort(m_port).build();
			SocketOptions options = new SocketOptions().setReadTimeoutMillis(m_readtimeout);
			Cluster cluster = Cluster.builder()
									 .addContactPoints(m_server.split(","))
									 .withPort(m_port)
									 .withSocketOptions(options)
									 .build();
			// Configure our LocalDateCodec
			CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
			codecRegistry.register(InstantCodec.instance); // maps Instant to time stamp;
			// codecRegistry.register(LocalDateCodec.instance); // maps LocalDate to date;
			// codecRegistry.register(LocalTimeCodec.instance); // maps LocalTime to time.
			
			m_session = cluster.connect(m_keySpace);
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
