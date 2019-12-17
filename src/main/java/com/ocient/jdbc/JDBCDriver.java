package com.ocient.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.jar.Manifest;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.util.logging.Level;

public class JDBCDriver implements Driver
{

	private static String version;
	private static final Logger LOGGER = Logger.getLogger( "com.ocient.jdbc" );
	private String logFileName;
	private FileHandler logHandler;

	static
	{
		try
		{
			final Class cls = JDBCDriver.class;
			final String clsPath = cls.getResource(cls.getSimpleName() + ".class").toString();
			final String mPath = clsPath.substring(0, clsPath.lastIndexOf("!") + 1) + "/META-INF/MANIFEST.MF";
			final InputStream i = new URL(mPath).openStream();
			version = (new Manifest(i)).getMainAttributes().getValue("Implementation-Version");
			i.close();

			DriverManager.registerDriver(new JDBCDriver());
		}
		catch (final Exception e)
		{
			e.printStackTrace(System.err);
		}
	}

	@Override
	public boolean acceptsURL(final String arg0) throws SQLException {
		final String protocol = arg0.substring(0, 14);
		if (!protocol.equals("jdbc:ocient://"))
		{
			return false;
		}

		return true;
	}

	@Override
	public Connection connect(final String arg0, final Properties arg1) throws SQLException {
		try
		{
			configLogger(arg1);
			
			final String protocol = arg0.substring(0, 14);
			if (!protocol.equals("jdbc:ocient://"))
			{
				return null;
			}

			final int portDelim = arg0.indexOf(":", "jdbc:ocient://".length());
			if (portDelim < 0)
			{
				throw SQLStates.MALFORMED_URL.clone();
			}
			final int dbDelim = arg0.indexOf("/", portDelim);
			if (dbDelim < 0)
			{
				throw SQLStates.MALFORMED_URL.clone();
			}
			final String hostname = arg0.substring("jdbc:ocient://".length(), portDelim);
			final String port = arg0.substring(portDelim + 1, dbDelim);
			final String db = arg0.substring(dbDelim + 1);
			int portNum = 0;
			try
			{
				portNum = Integer.parseInt(port);
			}
			catch (final Exception e)
			{
				throw SQLStates.MALFORMED_URL.clone();
			}

			Socket sock = null;
			try
			{
				sock = new Socket();
				sock.setReceiveBufferSize(4194304);
				sock.setSendBufferSize(4194304);
				sock.connect(new InetSocketAddress(hostname, portNum), 10000);
			}
			catch (final Throwable e)
			{
				try
				{
					sock.close();
				}
				catch (final IOException f)
				{}

				final SQLException g = SQLStates.FAILED_CONNECTION.clone();
				final Exception connInfo = new Exception("Connection failed connecting to " + hostname + ":" + portNum);
				g.initCause(connInfo);
				connInfo.initCause(e);
				throw g;
			}

			return new XGConnection(sock, arg1.getProperty("user"), arg1.getProperty("password"), portNum, arg0, db,
					version, arg1.getProperty("force", "false"));
		}
		catch (final Exception e)
		{
			if (e instanceof SQLException)
			{
				throw (SQLException) e;
			}
			
			throw SQLStates.newGenericException(e);
		}
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
		String loglevel = props.getProperty("loglevel");
		if (loglevel != null) {
			if (loglevel.equalsIgnoreCase("OFF")) {
				LOGGER.setLevel(Level.OFF);
			} else if (loglevel.equalsIgnoreCase("DEBUG")) {
				LOGGER.setLevel(Level.ALL);
			} else if (loglevel.equalsIgnoreCase("ERROR")) {
				LOGGER.setLevel(Level.WARNING);
			}
		}
		
		String logfile = props.getProperty("logfile");

		/* If logfile hasn't changed, return */
		if (((logfile == null) && (logFileName == null)) ||
			logfile.equals(logFileName)) {
			return;
		}

		/* Clean up the old handler */
		if (logHandler != null) {
			LOGGER.removeHandler(logHandler);
			logHandler = null;
			logFileName = null;
		}

		/* If we don't have a new log file, we're done */
		if (logfile == null) {
			return;
		}

		try {
			logHandler = new FileHandler(logfile);
			logHandler.setFormatter(new SimpleFormatter());
			logFileName = logfile;
			LOGGER.addHandler(logHandler);
		} catch (final IOException e) {
			e.printStackTrace(System.err);
		}
	}
}
