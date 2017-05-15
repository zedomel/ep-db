package ep.db.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;

public class Database {
	
	private final Properties config;
	
	private final DataSource ds;

	public Database(Properties configuration) {
		this.config = configuration;
		this.ds = initializeDataSource();
	}

	private DataSource initializeDataSource() {
		PGSimpleDataSource ds = new PGSimpleDataSource();
		ds.setServerName(config.getProperty("db.host"));
		ds.setDatabaseName(config.getProperty("db.database"));
		ds.setPortNumber(Integer.parseInt(config.getProperty("db.port")));
		ds.setUser(config.getProperty("db.user"));
		ds.setPassword(config.getProperty("db.password"));
		return ds;
	}

	public Connection getConnection() throws SQLException {
		return ds.getConnection();
	}

}
