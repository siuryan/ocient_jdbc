package com.ocient.jdbc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.SimpleFormatter;
import java.util.logging.Level;

public class JDBCDriver implements Driver
{

	private static String version = "7.0.1";
	private static final Logger LOGGER = Logger.getLogger( "com.ocient.jdbc" );
	private static String logFileName;
	private static FileHandler logHandler;
	private static final Boolean logMonitor = false;

	static
	{
		try
		{
			DriverManager.registerDriver(new JDBCDriver());
		}
		catch (final SQLException e)
		{
			e.printStackTrace(System.err);
		}
	}

	@Override
	public boolean acceptsURL(final String arg0) throws SQLException {
		if (arg0.length() < 14)
		{
			return false;
		}
		
		final String protocol = arg0.substring(0, 14);
		if (!protocol.equals("jdbc:ocient://"))
		{
			return false;
		}

		return true;
	}


	private Connection createConnection(final String hostname, int portNum, final String database, Properties properties) throws SQLException {
		Socket sock = null;
		String user = properties.getProperty("user");
		String pwd = properties.getProperty("password");
		String force = properties.getProperty("force");
		String tlsStr = properties.getProperty("tls","OFF").toUpperCase();
		XGConnection.Tls tls = XGConnection.Tls.valueOf(tlsStr);
		XGConnection conn = null;
		
		try
		{
			final InetAddress[] addrs = InetAddress.getAllByName(hostname);
			LOGGER.log(Level.INFO, String.format(
					"Received %d IP addresses for hostname %s", addrs.length, hostname));
			boolean connected = false;
			Throwable lastError = null;
			for (InetAddress addr : addrs) {
				try {
					final String url = "jdbc:ocient://" + hostname + ":" + Integer.toString(portNum) + "/" + database;
					conn = new XGConnection(user, pwd, addr.getHostAddress(), portNum, url, database, version, force, tls);
					LOGGER.log(Level.INFO,"About to attempt connection");
					conn.connect();
					LOGGER.log(Level.INFO,"Successfully connected");
					connected = true;
					break;
				} catch (final Throwable e) {
					conn = null;
					lastError = e;
					LOGGER.log(Level.WARNING, String.format(
							"Failed connecting to %s with exception %s with message %s", addr.toString(), e.toString(), e.getMessage()));
				}
			}
			if (!connected && lastError != null) {
				// Represents failure to connect.  Socket will be cleaned up in catch block.
				throw lastError;
			}
		}
		catch (final Throwable e)
		{
			final SQLException g = SQLStates.FAILED_CONNECTION.clone();
			final Exception connInfo = new Exception("Connection failed connecting to " + hostname + ":" + portNum + " - " + e.getMessage());
			g.initCause(connInfo);
			connInfo.initCause(e);
			throw g;
		}

		return conn;
	}

	@Override
	public Connection connect(final String arg0, Properties arg1) throws SQLException {
		try
		{
			final String protocol = arg0.substring(0, 14);
			if (!protocol.equals("jdbc:ocient://"))
			{
				return null;
			}

			final int dbDelim = arg0.indexOf("/", "jdbc:ocient://".length());
			if (dbDelim < 0)
			{
				throw SQLStates.MALFORMED_URL.clone();
			}

			final String hosts = arg0.substring("jdbc:ocient://".length(), dbDelim);
			final String[] hostList = hosts.split(",");
			String db = "";
			
			//Check for properties
			int propertyDelim = arg0.indexOf(";");
			if (propertyDelim > 0)
			{
				db = arg0.substring(dbDelim + 1, propertyDelim);
			}
			else
			{
				db = arg0.substring(dbDelim + 1);
			}
			
			while (propertyDelim > 0)
			{
				//Get the property name
				int equalPos = arg0.indexOf("=", propertyDelim+1);
				if (equalPos < 0)
				{
					throw SQLStates.MALFORMED_URL.clone();
				}
				
				String key = arg0.substring(propertyDelim+1, equalPos);
				
				//Find the end of this property
				int propertyEnd = arg0.indexOf(";", equalPos+1);
				if (propertyEnd < 0)
				{
					propertyEnd = arg0.length();
				}
				
				String value = arg0.substring(propertyDelim+1 + key.length() + 1, propertyEnd);
				arg1.setProperty(key, value);
				
				if (propertyEnd == arg0.length())
				{
					propertyDelim = -1;
				}
				else
				{
					propertyDelim = propertyEnd;
				}
			}
			
			if (arg1.getProperty("force") == null)
			{
				arg1.setProperty("force", "false");
			}

			configLogger(arg1);
			Exception lastException = null;
			for (String host : hostList) {
				final String[] hostnameAndPort = host.split(":");
				if(hostnameAndPort.length != 2) 
				{
					LOGGER.log(Level.SEVERE, "Host list in URL is malformed");
					throw SQLStates.MALFORMED_URL.clone();
				} 

				int portNum = 0;
				try
				{
					portNum = Integer.parseInt(hostnameAndPort[1]);
				}
				catch (final Exception e)
				{
					LOGGER.log(Level.SEVERE, "Port number in URL was not an integer");
					throw SQLStates.MALFORMED_URL.clone();
				}

				try {
					Connection conn = createConnection(hostnameAndPort[0], portNum, db, arg1);
					return conn;
				} catch (final Exception e) {
					lastException = e;
				}
			}

			if (lastException != null) {
				throw lastException;
			}
		}
		catch (final Exception e)
		{
			if (e instanceof SQLException)
			{
				throw (SQLException) e;
			}

			throw SQLStates.newGenericException(e);
		}

		//if we get it here, it is a malformed URL
		throw SQLStates.MALFORMED_URL.clone();
	}

	public String getDriverVersion() {
		return version;
	}

	@Override
	public int getMajorVersion() {
		return Integer.parseInt(version.substring(0, version.indexOf(".")));
	}

	@Override
	public int getMinorVersion() {
		final int i = version.indexOf(".") + 1;
		return Integer.parseInt(version.substring(i, version.indexOf(".", i)));
	}

	@Override
	public Logger getParentLogger() {
		return LOGGER;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(final String arg0, final Properties arg1) throws SQLException {
		final DriverPropertyInfo[] retval = new DriverPropertyInfo[4];
		final DriverPropertyInfo user = new DriverPropertyInfo("user", null);
		user.description = "The userid to use for the connection";
		user.required = true;
		retval[0] = user;

		final DriverPropertyInfo pwd = new DriverPropertyInfo("password", null);
		pwd.description = "The password to use for the connection";
		pwd.required = true;
		retval[1] = pwd;

		final DriverPropertyInfo loglevel = new DriverPropertyInfo("loglevel", null);
		loglevel.description = "Logging Level";
		loglevel.required = false;
		loglevel.choices = new String[3];
		loglevel.choices[0] = "OFF";
		loglevel.choices[1] = "ERROR";
		loglevel.choices[2] = "DEBUG";

		retval[2] = loglevel;

		final DriverPropertyInfo logfile = new DriverPropertyInfo("logfile", null);
		logfile.description = "Log file";
		logfile.required = false;
		retval[3] = logfile;

		return retval;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	private void configLogger(final Properties props) {
		synchronized(logMonitor)
		{
			String loglevel = props.getProperty("loglevel");
			String logfile = props.getProperty("logfile");
			if (loglevel == null || logfile == null)
			{
				LOGGER.setLevel(Level.OFF);
				return;
			}
			LOGGER.log(Level.INFO, String.format("New logger settings. LogLevel: %s. LogFile: %s",loglevel, logfile));
			
			if (loglevel != null) {
				if (loglevel.equalsIgnoreCase("OFF")) {
					LOGGER.setLevel(Level.OFF);
					return;
				} else if (loglevel.equalsIgnoreCase("DEBUG")) {
					LOGGER.setLevel(Level.ALL);
				} else if (loglevel.equalsIgnoreCase("ERROR")) {
					LOGGER.setLevel(Level.WARNING);
				}
			}
	
			/* If logfile hasn't changed, return */
			if (logfile.equals(logFileName)) {
				return;
			}
	
			/* Clean up the old handler */
			LOGGER.setUseParentHandlers(false);
			LOGGER.log(Level.INFO, "Resetting logger");
			Handler[] handlers = LOGGER.getHandlers();
			for(Handler handler : handlers) {
			    LOGGER.removeHandler(handler);
			}
	
			try {
				logHandler = new FileHandler(logfile, true);
				logHandler.setFormatter(new ThreadFormatter());
				logFileName = logfile;
				LOGGER.addHandler(logHandler);
				LOGGER.log(Level.INFO, "Enabling logger");
			} catch (final IOException | IllegalArgumentException e) {
				e.printStackTrace(System.err);
				// An illegal file argument was entered most likely.
				LOGGER.setLevel(Level.OFF);
				return;
			}
		}
	}
}
