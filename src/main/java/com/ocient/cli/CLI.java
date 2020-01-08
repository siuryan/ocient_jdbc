package com.ocient.cli;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections4.bag.TreeBag;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import com.google.common.collect.TreeMultimap;
import com.ocient.jdbc.DataEndMarker;
import com.ocient.jdbc.XGConnection;
import com.ocient.jdbc.XGDatabaseMetaData;
import com.ocient.jdbc.XGStatement;
import com.ocient.jdbc.proto.ClientWireProtocol.SysQueriesRow;
import com.ocient.jdbc.proto.PlanProtocol.PlanMessage;

public class CLI {
	private static Connection conn;
	private static boolean timing = false;
	private static boolean trace = false;
	private static boolean performance = false;
	private static String outputCSVFile = "";
	private static String db;
	private static String user;
	private static String pwd;
	private static Collection<File> sources = new HashSet<File>();

	private static char quote = '\0';
	private static boolean comment = false;

	private static int BAG_FLUSH_SIZE = 8192;
	private static int MAX_BAGS_SIZE_PER_READER = 64 * 1024 * 1024;

	public static void main(final String[] args) {
		Terminal terminal;
		LineReader reader;
		try {
			Class.forName("com.ocient.jdbc.JDBCDriver");
		} catch (final Exception e) {
			System.out.println("Unable to load JDBC driver!");
			System.exit(1);
		}

		boolean echo = false;
		final Console cons = System.console();
		try {
			if (cons == null) {
				echo = true;
			}
			terminal = TerminalBuilder.builder().system(true).build();
			reader = LineReaderBuilder.builder().terminal(terminal).build();
		} catch (final IOException e) {
			System.out.println("Error setting up console");
			return;
		}

		// Usernames and passwords can contain any characters if appropriately quoted
		// (usernames use SQL case-sensitivity).
		// Quote for shell. Subsequent arguments are dropped.

		if (args.length == 0) {
			user = reader.readLine("Username: ");
		} else {
			user = args[0];
		}
		if (args.length < 2) {
			if (echo) {
				System.out.println();
			}
			pwd = reader.readLine("Password: ", '\0');
		} else {
			pwd = args[1];
		}
		System.out.println();

		boolean quit = false;
		String cmd = "";

		try {
			while (true) {
				// jline has ways to handle this, but they're underdocumented and overbuilt to
				// the point of obscenity
				if (!quit)
					cmd = scrubCommand(reader.readLine("Ocient> ") + " ");

				while (true) {
					if (quit || cmd.trim().equalsIgnoreCase("QUIT")) {
						try {
							if (conn != null && !conn.isClosed()) {
								conn.close();
							}
						} catch (final Exception e) {
						}
						if (echo) {
							System.out.println();
						}
						return;
					}
					if (echo) {
						System.out.println();
					}

					if (!comment && quote == '\0' && cmd.trim().endsWith(";")) {
						// System.out.println("Finished scrubbing command: '" + cmd + "'");
						cmd = cmd.trim();
						cmd = cmd.substring(0, cmd.length() - 1).replaceAll("\\s+", " ").trim();
						if (trace && !endsWithIgnoreCase(cmd, " trace")) {
							cmd = cmd + " trace";
						}
						// System.out.println("Finished trimming scrubbed: '" + cmd + "'");
						break;
					} else {
						// System.out.println("Current command text: '" + cmd + "'");
						cmd += scrubCommand(reader.readLine("(cont)> ") + " ");
					}
				}

				quit = processCommand(cmd);
			}
		} catch (final UserInterruptException | EndOfFileException e) {
			return;
		}
	}

	private static String scrubCommand(String cmd) {
		final StringBuilder out = new StringBuilder(256);
		int i = 0;
		int length = cmd.length();
		while (i < length) {
			char c = cmd.charAt(i);
			if (!comment) {
				if (quote == '\0' && (i + 1) != length) {
					if (c == '-' && cmd.charAt(i + 1) == '-') {
						break;
					}
					if (c == '/' && cmd.charAt(i + 1) == '*') {
						comment = true;
						i++;
						continue;
					}
				}
				if ((c == '\'' || c == '"') && (quote == '\0' || quote == c)) // char is an active quote
				{
					quote = (quote == '\0') ? c : '\0';
				}
				out.append(c);
			} else if ((i + 1) != length && c == '*' && cmd.charAt(i + 1) == '/') {
				comment = false;
				i++;
			}

			i++;
		}
		return out.toString();
	}

	private static boolean processCommand(String cmd) {
		boolean quit = false;
		// System.out.println("processCommand(" + cmd + ")");
		if (cmd.equals("")) {
			return quit;
		}
		if (startsWithIgnoreCase(cmd, "CONNECT TO")) {
			connectTo(cmd);
		} else if (startsWithIgnoreCase(cmd, "SELECT") || startsWithIgnoreCase(cmd, "WITH")) {
			select(cmd);
		} else if (cmd.equalsIgnoreCase("TIMING ON")) {
			timing = true;
		} else if (cmd.equalsIgnoreCase("TIMING OFF")) {
			timing = false;
		} else if (cmd.equalsIgnoreCase("PERFORMANCE ON")) {
			performance = true;
			timing = true;
		} else if (cmd.equalsIgnoreCase("PERFORMANCE OFF")) {
			performance = false;
			timing = false;
		} else if (cmd.equalsIgnoreCase("TRACE ON")) {
			trace = true;
		} else if (cmd.equalsIgnoreCase("TRACE OFF")) {
			trace = false;
		} else if (startsWithIgnoreCase(cmd, "SET SCHEMA")) {
			setSchema(cmd);
		} else if (cmd.equalsIgnoreCase("GET SCHEMA")) {
			getSchema(cmd);
		} else if (startsWithIgnoreCase(cmd, "EXPLAIN")) {
			explain(cmd);
		} else if (startsWithIgnoreCase(cmd, "CREATE") || startsWithIgnoreCase(cmd, "DROP")
				|| startsWithIgnoreCase(cmd, "ALTER") || startsWithIgnoreCase(cmd, "TRUNCATE")) {
			update(cmd);
		} else if (startsWithIgnoreCase(cmd, "LOAD INTO")) {
			loadFromFiles(cmd);
		} else if (startsWithIgnoreCase(cmd, "LIST TABLES")) {
			listTables(cmd, false);
		} else if (startsWithIgnoreCase(cmd, "LIST SYSTEM TABLES")) {
			listTables(cmd, true);
		} else if (startsWithIgnoreCase(cmd, "LIST VIEWS")) {
			listViews(cmd);
		} else if (startsWithIgnoreCase(cmd, "DESCRIBE TABLE")) {
			describeTable(cmd);
		} else if (startsWithIgnoreCase(cmd, "DESCRIBE VIEW")) {
			describeView(cmd);
		} else if (startsWithIgnoreCase(cmd, "LIST INDICES") || startsWithIgnoreCase(cmd, "LIST INDEXES")) {
			listIndexes(cmd);
		} else if (startsWithIgnoreCase(cmd, "PLAN EXECUTE")) {
			executePlan(cmd);
		} else if (startsWithIgnoreCase(cmd, "PLAN EXPLAIN")) {
			explainPlan(cmd);
		} else if (startsWithIgnoreCase(cmd, "PLAN LIST")) {
			listPlan();
		} else if (startsWithIgnoreCase(cmd, "SOURCE")) {
			quit = source(cmd);
		} else if (startsWithIgnoreCase(cmd, "CANCEL")) {
			cancelQuery(cmd);
		} else if (startsWithIgnoreCase(cmd, "KILL")) {
			killQuery(cmd);
		} else if (startsWithIgnoreCase(cmd, "LIST ALL QUERIES")) {
			listAllQueries();
		} else if (startsWithIgnoreCase(cmd, "OUTPUT NEXT QUERY")) {
			outputNextQuery(cmd);
		} else if (startsWithIgnoreCase(cmd, "FORCE EXTERNAL")) {
			forceExternal(cmd);
		} else if (startsWithIgnoreCase(cmd, "EXPORT TABLE")) {
			exportTable(cmd);
		} else {
			System.out.println("Invalid command: " + cmd);
		}

		return quit;
	}

	private static boolean startsWithIgnoreCase(final String in, final String cmp) {
		int firstNonParentheses = 0;
		int len = in.length();
		while (firstNonParentheses < len && in.charAt(firstNonParentheses) == '(') {
			firstNonParentheses++;
		}
		if (in.substring(firstNonParentheses).toUpperCase().startsWith(cmp.toUpperCase())) {
			return true;
		}

		return false;
	}

	private static boolean endsWithIgnoreCase(final String in, final String cmp) {
		return in.toUpperCase().endsWith(cmp.toUpperCase());
	}

	// Generate a regex for an unquoted alphanumeric ([a-zA-Z0-9_]) or quoted free
	// (.) token. Reluctant.
	// Do not insert multiple regexes for tokens of the same name (or "q0" + another
	// name) into a single pattern.
	private static String tk(String name) {
		return "(?<q0" + name + ">\"?)(?<" + name + ">(\\w+?|(?<=\").+?(?=\")))\\k<q0" + name + ">";
	}

	// Generate a regex for an unquoted alphanumeric ([a-zA-Z0-9_]) or quoted free
	// (.) token possibly followed
	// by @ and more unquoted alphanumeric ([a-zA-Z0-9_]) or quoted free (.) tokens.
	private static String userTk() {
		return "(?<q0user>\"?)(?<user>(\\w+?|(?<=\").+?(?=\"))(@(\\w+?|(?<=\").+?(?=\")))?)\\k<q0user>";
	}

	// Get a token from its generated regex according to SQL case-sensitivity rules
	// (sensitive iff quoted).
	// Do not call on a matcher that has not yet called matches().
	private static String getTk(Matcher m, String name, String def) {
		if (m.group(name) == null) {
			return def;
		}
		if (m.group("q0" + name).length() == 0) {
			return m.group(name).toLowerCase();
		}
		return m.group(name);
	}

	private static Pattern connectToSyntax = Pattern.compile(
			"connect to (?<url>.+?)(?<up> user (" + userTk() + ") using (?<q>\"?)(?<pwd>.+?)\\k<q>)?(?<force> force)?",
			Pattern.CASE_INSENSITIVE);

	private static void connectTo(final String cmd) {
		if (isConnected()) {
			try {
				conn.close();
			} catch (final Exception e) {
				System.out.println("Error: " + e.getMessage());
				return;
			}
		}

		try {
			final Matcher m = connectToSyntax.matcher(cmd);
			if (!m.matches()) {
				System.out.println("Syntax: connect to <jdbc url>( user <username> using <password>)?( force)?");
				return;
			}
			if (m.group("up") == null) {
				doConnect(user, pwd, (m.group("force") != null), m.group("url"));
			} else {
				doConnect(getTk(m, "user", null), m.group("pwd"), (m.group("force") != null), m.group("url"));
			}

		} catch (final Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}

	private static void doConnect(final String user, final String pwd, final boolean force, final String url)
			throws Exception {
		final Properties prop = new Properties();
		prop.setProperty("user", user);
		prop.setProperty("password", pwd);
		prop.setProperty("force", force ? "true" : "false");
		conn = DriverManager.getConnection(url, prop);
		CLI.db = ((XGConnection) conn).getDB();
	}

	private static boolean isConnected() {
		if (conn != null) {
			return ((XGConnection) conn).connected();
		}
		return false;
	}

	private static void getSchema(final String cmd) {
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}

		try {
			String schema = conn.getSchema();
			if (!schema.toLowerCase().equals(schema)) {
				schema = "\"" + schema + "\"";
			}
			System.out.println(schema);
		} catch (final Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}

	private static void setSchema(final String cmd) {
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}

		try {
			String schema = cmd.substring(11).trim();
			if (schema.startsWith("\"")) {
				if (!schema.endsWith("\"")) {
					System.out.println("Unclosed quotes!");
					return;
				}

				schema = schema.substring(1, schema.length() - 1);
			}
			conn.setSchema(schema);
		} catch (final Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}

	private static void forceExternal(final String cmd) {
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		} else if (!cmd.toLowerCase().endsWith("on") && !cmd.toLowerCase().endsWith("off")) {
			System.out.println("force external command requires argument \"on\" or \"off\"");
			return;
		}

		boolean force = cmd.toLowerCase().endsWith("on");
		try {
			((XGConnection) conn).forceExternal(force);
		} catch (final Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}

	private static Pattern listTablesSyntax = Pattern.compile("list tables(?<verbose> verbose)?",
			Pattern.CASE_INSENSITIVE);

	private static Pattern listSystemTablesSyntax = Pattern.compile("list system tables(?<verbose> verbose)?",
			Pattern.CASE_INSENSITIVE);

	private static void listTables(final String cmd, boolean isSystemTables) {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}

		ResultSet rs = null;

		try {
			final DatabaseMetaData dbmd = conn.getMetaData();
			final Matcher m = isSystemTables ? listSystemTablesSyntax.matcher(cmd) : listTablesSyntax.matcher(cmd);
			if (!m.matches()) {
				// this line will never be reached,
				// but we have to call matches() anyway
				return;
			}

			start = System.currentTimeMillis();
			if (isSystemTables) {
				final XGDatabaseMetaData xgdbmd = (XGDatabaseMetaData) dbmd;
				rs = xgdbmd.getSystemTables("", "%", "%", new String[0]);
			} else {
				rs = dbmd.getTables("", "%", "%", new String[0]);
			}
			final ResultSetMetaData meta = rs.getMetaData();

			if (m.group("verbose") != null) {
				printResultSet(rs, meta);
			} else {
				final StringBuilder line = new StringBuilder(1024);
				while (rs.next()) {
					line.append(rs.getString("TABLE_SCHEM"));
					line.append(".");
					line.append(rs.getString("TABLE_NAME"));
					line.append(", ");
				}
				if (line.length() != 0) {
					line.setLength(line.length() - 2);
					System.out.println(line);
				}
			}
			printWarnings(rs);
			end = System.currentTimeMillis();

			rs.close();

			printTime(start, end);
		} catch (final Exception e) {
			try {
				rs.close();
			} catch (Exception f) {
			}

			System.out.println("Error: " + e.getMessage());
		}
	}

	private static void exportTable(final String cmd) {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}
		try {
			final Statement stmt = conn.createStatement();
			start = System.currentTimeMillis();
			System.out.println(((XGStatement) stmt).exportTable(cmd));
			printWarnings(stmt);
			end = System.currentTimeMillis();

			stmt.close();

			printTime(start, end);
		} catch (final Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}

	private static Pattern listViewsSyntax = Pattern.compile("list views(?<verbose> verbose)?",
			Pattern.CASE_INSENSITIVE);

	private static void listViews(final String cmd) {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}

		ResultSet rs = null;

		try {
			final DatabaseMetaData md = conn.getMetaData();
			final XGDatabaseMetaData dbmd = (XGDatabaseMetaData) md;
			final Matcher m = listViewsSyntax.matcher(cmd);
			if (!m.matches()) {
				// this line will never be reached,
				// but we have to call matches() anyway
				return;
			}

			start = System.currentTimeMillis();
			rs = dbmd.getViews("", "%", "%", new String[0]);
			final ResultSetMetaData meta = rs.getMetaData();

			if (m.group("verbose") != null) {
				printResultSet(rs, meta);
			} else {
				final StringBuilder line = new StringBuilder(1024);
				while (rs.next()) {
					line.append(rs.getString("VIEW_SCHEM"));
					line.append(".");
					line.append(rs.getString("VIEW_NAME"));
					line.append(", ");
				}
				if (line.length() != 0) {
					line.setLength(line.length() - 2);
					System.out.println(line);
				}
			}
			printWarnings(rs);
			end = System.currentTimeMillis();

			rs.close();

			printTime(start, end);
		} catch (final Exception e) {
			try {
				rs.close();
			} catch (Exception f) {
			}

			System.out.println("Error: " + e.getMessage());
		}
	}

	private static Pattern describeTableSyntax = Pattern.compile(
			"describe( table )?((" + tk("schema") + ")\\.)?(" + tk("table") + ")(?<verbose> verbose)?",
			Pattern.CASE_INSENSITIVE);

	private static void describeTable(final String cmd) {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}

		ResultSet rs = null;

		try {
			final Matcher m = describeTableSyntax.matcher(cmd);
			if (!m.matches()) {
				System.out.println("Syntax: describe (<schema>.)?<table>");
				return;
			}

			start = System.currentTimeMillis();
			final DatabaseMetaData dbmd = conn.getMetaData();
			rs = dbmd.getColumns("", getTk(m, "schema", conn.getSchema()), getTk(m, "table", null), "%");
			final ResultSetMetaData meta = rs.getMetaData();

			if (m.group("verbose") != null) {
				printResultSet(rs, meta);
			} else {
				final StringBuilder line = new StringBuilder(1024);
				while (rs.next()) {
					line.append(rs.getString("COLUMN_NAME"));
					line.append(" (");
					line.append(rs.getString("TYPE_NAME"));
					line.append("), ");
				}
				if (line.length() != 0) {
					line.setLength(line.length() - 2);
					System.out.println(line);
				}
			}
			printWarnings(rs);
			end = System.currentTimeMillis();

			rs.close();

			printTime(start, end);
		} catch (final Exception e) {
			try {
				rs.close();
			} catch (Exception f) {
			}

			System.out.println("Error: " + e.getMessage());
		}
	}

	private static Pattern describeViewSyntax = Pattern.compile(
			"describe( view )?((" + tk("schema") + ")\\.)?(" + tk("view") + ")(?<verbose> verbose)?",
			Pattern.CASE_INSENSITIVE);

	private static void describeView(final String cmd) {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}

		ResultSet rs = null;

		try {
			final Matcher m = describeViewSyntax.matcher(cmd);
			if (!m.matches()) {
				System.out.println("Syntax: describe view (<schema>.)?<view>");
				return;
			}

			start = System.currentTimeMillis();
			final DatabaseMetaData md = conn.getMetaData();
			final XGDatabaseMetaData dbmd = (XGDatabaseMetaData) md;
			rs = dbmd.getViews("", getTk(m, "schema", conn.getSchema()), getTk(m, "view", null), null);
			final ResultSetMetaData meta = rs.getMetaData();

			if (m.group("verbose") != null)
				printResultSet(rs, meta);

			else {
				final StringBuilder line = new StringBuilder(1024);

				while (rs.next())
					line.append(rs.getString("VIEW_QUERY_TEXT"));

				if (line.length() != 0) {
					line.setLength(line.length() - 2);
					System.out.println(line);
				}
			}
			printWarnings(rs);
			end = System.currentTimeMillis();
			rs.close();

			printTime(start, end);
		} catch (final Exception e) {
			try {
				rs.close();
			} catch (Exception f) {
			}

			System.out.println("Error: " + e.getMessage());
		}
	}

	private static Pattern listIndexesSyntax = Pattern.compile(
			"list ind(ic|ex)es ((" + tk("schema") + ")\\.)?(" + tk("table") + ")(?<verbose> verbose)?",
			Pattern.CASE_INSENSITIVE);

	private static void listIndexes(final String cmd) {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}

		ResultSet rs = null;

		try {
			final Matcher m = listIndexesSyntax.matcher(cmd);
			if (!m.matches()) {
				System.out.println("Syntax: list indexes (<schema>.)?<table>");
				return;
			}

			start = System.currentTimeMillis();
			final DatabaseMetaData dbmd = conn.getMetaData();
			// this behavior is slightly different from the jdbc call itself--
			// the call allows schema to be null, in which case it doesn't filter on it.
			// we assume the current schema for convenience & to limit results to one table
			rs = dbmd.getIndexInfo("", getTk(m, "schema", conn.getSchema()), getTk(m, "table", null), false, false);
			final ResultSetMetaData meta = rs.getMetaData();

			if (m.group("verbose") != null) {
				printResultSet(rs, meta);
			} else {
				final StringBuilder line = new StringBuilder(1024);
				String currIndex = "";
				while (rs.next()) {
					final String nextIndex = rs.getString("INDEX_NAME");
					if (!nextIndex.equals(currIndex)) {
						currIndex = nextIndex;
						if (line.length() > 0) {
							line.setLength(line.length() - 2);
							line.append("), ");
						}
						line.append(currIndex);
						line.append(" (");
					}
					line.append(rs.getString("COLUMN_NAME"));
					line.append(", ");
				}
				if (line.length() != 0) {
					line.setLength(line.length() - 2);
					line.append(")");
					System.out.println(line);
				}
			}

			printWarnings(rs);
			end = System.currentTimeMillis();
			rs.close();

			printTime(start, end);
		} catch (final Exception e) {
			try {
				rs.close();
			} catch (Exception f) {
			}

			System.out.println("Error: " + e.getMessage());
		}
	}

	private static void select(final String cmd) {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}

		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.createStatement();
			start = System.currentTimeMillis();
			rs = stmt.executeQuery(cmd);
			final ResultSetMetaData meta = rs.getMetaData();

			if (outputCSVFile.isEmpty()) {
				printResultSet(rs, meta);
			} else {
				outputResultSet(rs, meta);
				outputCSVFile = "";
			}
			printWarnings(stmt);
			printWarnings(rs);
			end = System.currentTimeMillis();

			rs.close();
			stmt.close();

			printTime(start, end);
		} catch (final Exception e) {
			try {
				rs.close();
				stmt.close();
			} catch (Exception f) {
			}
			System.out.println("Error: " + e.getMessage());
		}
	}

	private static void explain(final String cmd) {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}
		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.createStatement();
			start = System.currentTimeMillis();
			rs = stmt.executeQuery(cmd);

			while (rs.next()) {
				String planLine = rs.getString(1);
				System.out.println(planLine);
			}
			end = System.currentTimeMillis();

			rs.close();
			stmt.close();

			printTime(start, end);
		} catch (final Exception e) {
			try {
					rs.close();
					stmt.close();
			} catch (Exception f) {
			}
			System.out.println("Error: " + e.getMessage());
		}
	}

	private static void executePlan(final String cmd) {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}

		Statement stmt = null;
		ResultSet rs = null;
		String plan = cmd.substring("PLAN EXECUTE ".length()).trim();

		if (startsWithIgnoreCase(plan, "INLINE ")) {
			plan = plan.substring("INLINE ".length()).trim();

			try {
				stmt = conn.createStatement();
				start = System.currentTimeMillis();
				rs = ((XGStatement) stmt).executeInlinePlan(plan);
				final ResultSetMetaData meta = rs.getMetaData();

				printResultSet(rs, meta);
				printWarnings(stmt);
				printWarnings(rs);
				end = System.currentTimeMillis();

				rs.close();
				stmt.close();

				printTime(start, end);
			} catch (final Exception e) {
				try {
					rs.close();
					stmt.close();
				} catch (Exception f) {
				}
				System.out.println("Error: " + e.getMessage());
			}
		} else {
			try {
				stmt = conn.createStatement();
				start = System.currentTimeMillis();
				rs = ((XGStatement) stmt).executePlan(plan);
				final ResultSetMetaData meta = rs.getMetaData();

				printResultSet(rs, meta);
				printWarnings(stmt);
				printWarnings(rs);
				end = System.currentTimeMillis();

				rs.close();
				stmt.close();

				printTime(start, end);
			} catch (final Exception e) {
				try {
					rs.close();
					stmt.close();
				} catch (Exception f) {
				}
				System.out.println("Error: " + e.getMessage());
			}
		}
	}

	private static void explainPlan(final String cmd) {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}

		try {
			final Statement stmt = conn.createStatement();
			start = System.currentTimeMillis();
			final String plan = cmd.substring("PLAN EXPLAIN ".length()).trim();
			final PlanMessage pm = ((XGStatement) stmt).explainPlan(plan);

			System.out.println(pm);
			printWarnings(stmt);
			end = System.currentTimeMillis();

			stmt.close();

			printTime(start, end);
		} catch (final Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}

	private static void listPlan() {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}

		try {
			final Statement stmt = conn.createStatement();
			start = System.currentTimeMillis();
			final ArrayList<String> planNames = ((XGStatement) stmt).listPlan();
			if (planNames.size() > 0) {
				System.out.println("    Plan Name    ");
				System.out.println("-----------------");
				for (int i = 0; i < planNames.size(); ++i)
					System.out.println(planNames.get(i));
			}

			printWarnings(stmt);
			end = System.currentTimeMillis();

			stmt.close();

			printTime(start, end);
		} catch (final Exception e) {
			System.out.println("CLI Error: " + e.getMessage());
		}
	}

	private static void cancelQuery(final String cmd) {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}
		try {
			final Statement stmt = conn.createStatement();
			start = System.currentTimeMillis();
			final String uuid = cmd.substring("CANCEL ".length()).trim();
			((XGStatement) stmt).cancelQuery(uuid);
			printWarnings(stmt);
			end = System.currentTimeMillis();

			stmt.close();

			printTime(start, end);
		} catch (final Exception e) {
			System.out.println("CLI Error: " + e.getMessage());
		}
	}

	private static void killQuery(final String cmd) {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}
		try {
			final Statement stmt = conn.createStatement();
			start = System.currentTimeMillis();
			final String uuid = cmd.substring("KILL ".length()).trim();
			((XGStatement) stmt).killQuery(uuid);
			printWarnings(stmt);
			end = System.currentTimeMillis();

			stmt.close();

			printTime(start, end);
		} catch (final Exception e) {
			System.out.println("CLI Error: " + e.getMessage());
		}
	}

	private static void listAllQueries() {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}
		try {
			final Statement stmt = conn.createStatement();
			start = System.currentTimeMillis();
			final ArrayList<SysQueriesRow> queryList = ((XGStatement) stmt).listAllQueries();

			if (queryList.size() > 0) {
				printAllQueries(queryList);
			}

			printWarnings(stmt);
			end = System.currentTimeMillis();

			stmt.close();

			printTime(start, end);
		} catch (final Exception e) {
			System.out.println("CLI Error: " + e.getMessage());
		}
	}

	private static void outputNextQuery(final String cmd) {
		try {
			outputCSVFile = cmd.substring("OUTPUT NEXT QUERY ".length()).trim();
			if (outputCSVFile.isEmpty()) {
				System.out.println("Provide a filename to output the query to");
			}
		} catch (final Exception e) {
			System.out.println("CLI Error: " + e.getMessage());
		}
	}

	private static void printAllQueries(final ArrayList<SysQueriesRow> queries) {
		System.out.format("%-40s%-15s%-15s%-20s%-20s%-15s%-20s%-15s%s\n", "query id", "user", "importance",
				"estimated time", "elapsed time", "status", "server", "database", "sql");
		System.out.println(new String(new char[170]).replace("\0", "-"));
		for (SysQueriesRow row : queries) {
			System.out.format("%-40s%-15s%-15s%-20s%-20s%-15s%-20s%-15s%s\n", row.getQueryId(), row.getUserid(),
					row.getImportance(), row.getEstimatedTimeSec(), row.getElapsedTimeSec(), row.getStatus(),
					row.getQueryServer(), row.getDatabase(), row.getSqlText());
		}
	}

	private static void update(final String cmd) {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}

		try {
			final Statement stmt = conn.createStatement();
			start = System.currentTimeMillis();
			final long numRows = stmt.executeUpdate(cmd);
			end = System.currentTimeMillis();

			System.out.println("Modified " + numRows + (numRows == 1 ? " row" : " rows"));
			printWarnings(stmt);

			stmt.close();

			printTime(start, end);
		} catch (final Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}

	private static Pattern loadFromFilesSyntax = Pattern.compile(
			"load into ((" + tk("schema") + ")\\.)?(" + tk("table")
					+ ")( delimiter '(?<delim>.)')?( streams (?<nstr>\\d+?))? from (?<glob>.*)",
			Pattern.CASE_INSENSITIVE);

	/*
	 * Feed ingress from delimited file(s)
	 */
	private static void loadFromFiles(final String cmd) {
		long start = 0;
		long end = 0;
		if (!isConnected()) {
			System.out.println("No database connection exists");
			return;
		}

		try {
			final Matcher m = loadFromFilesSyntax.matcher(cmd);
			if (!m.matches()) {
				System.out.println(
						"Syntax: load into (<schema>.)?<table> (delimiter '<delimiter char>')? (streams <num streams>)? from <filename pattern>\n");
				return;
			}

			String schema = getTk(m, "schema", conn.getSchema());
			String table = getTk(m, "table", null);
			String delimiter = (m.group("delim") != null) ? m.group("delim") : "|"; // default delimiter
			String glob = m.group("glob").trim();
			int streams = (m.group("delim") != null) ? Integer.parseInt(m.group("nstr"))
					: Runtime.getRuntime().availableProcessors();
			if (streams < 1) {
				System.out.println("The number of streams cannot be less than one.");
				return;
			}

			start = System.currentTimeMillis();

			long numRows = 0; // Number of rows loaded
			// Now we need to get info for all the ingress nodes
			ArrayList<Endpoint> ingressNodes = new ArrayList<>(); // TODO = getIngressNodes();
			ingressNodes = pickNodesToTalkTo(ingressNodes, streams);

			// Verify that the table exists and get its metadata
			// TODO if (!ingressKnowsTable(ingressNodes, schema, table))
			// {
			// System.out.println("Table doesn't exist!");
			// return;
			// }

			// Get table metadata
			final TableMetadata tableMetadata = null; // TODO = getTableMetadataFromIngress(ingressNodes, schema,
														// table);

			// And figure out what files we are loading from
			final ArrayList<Path> files = processGlob(glob);
			if (files.size() == 0) {
				System.out.println("No input files!");
				return;
			}

			// Load from "files" to "ingressNodes" using "tableMetadata" and "delimiter"
			numRows = doLoadFromFiles(files, ingressNodes, tableMetadata, delimiter);

			end = System.currentTimeMillis();

			// Did the load abort?
			if (numRows == -1) {
				System.out.println("The load aborted due to an internal error");
				return;
			}

			System.out.println("Loaded " + numRows + (numRows == 1 ? " row" : " rows"));

			printTime(start, end);
		} catch (final Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}

	private static boolean sourceCommands(BufferedReader reader) throws IOException {
		boolean quit = false;

		try {
			while (true) {
				String line = reader.readLine();
				if (line == null)
					return quit;

				String cmd = scrubCommand(line + " ");

				while (true) {
					quit = cmd.trim().equalsIgnoreCase("QUIT");
					if (quit) {
						try {
							if (conn != null && !conn.isClosed())
								conn.close();
						} catch (final Exception e) {
						}
						return quit;
					}

					if (!comment && quote == '\0' && cmd.trim().endsWith(";")) {
						cmd = cmd.trim();
						cmd = cmd.substring(0, cmd.length() - 1).replaceAll("\\s+", " ").trim();
						if (trace && !endsWithIgnoreCase(cmd, " trace"))
							cmd = cmd + " trace";
						break;
					} else {
						String cont = reader.readLine();
						if (cont == null)
							return quit;

						cmd += scrubCommand(line + " ");
					}
				}

				quit = processCommand(cmd);
				if (quit)
					return quit;
			}
		} catch (final UserInterruptException | EndOfFileException e) {
		}

		return quit;

	}

	private static boolean source(final String cmd) {

		boolean quit = false;

		String[] tokens = cmd.split("\\s+");
		if (1 == tokens.length) {
			System.out.println(tokens[0] + " error: filename missing");
			return quit;
		}

		if (3 < tokens.length || 3 == tokens.length && !tokens[2].equalsIgnoreCase("TRACE")) {
			System.out.println("Error: too many parameters: " + cmd);
			return quit;
		}

		File file = new File(tokens[1]);
		boolean added = sources.add(file);
		if (!added) {
			System.out.println(tokens[0] + " error: " + file + " (source file already open)");
			return quit;
		}

		long start = System.currentTimeMillis();

		try {
			Reader reader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(reader);

			char oldQuote = quote;
			quote = '\0';

			boolean oldComment = comment;
			comment = false;

			if (3 == tokens.length)
				System.out.println(tokens[0] + ": Sourcing " + tokens[1]);

			try {
				quit = sourceCommands(bufferedReader);
			} catch (Throwable e) {
			}

			if (3 == tokens.length)
				System.out.println(tokens[0] + ": Closing " + tokens[1]);

			comment = oldComment;
			quote = oldQuote;

			bufferedReader.close();
			reader.close();
		} catch (Throwable e) {
			System.out.println(tokens[0] + " error: " + e.getMessage());
		}

		printTime(start, System.currentTimeMillis());

		boolean removed = sources.remove(file);

		return quit;
	}

	/*
	 * We have a set of ingress nodes that we know about and a desired number of
	 * streams to use.
	 *
	 * We need to figure out exactly what ingress nodes to use and get connections
	 * to them.
	 */
	private static ArrayList<Endpoint> pickNodesToTalkTo(final ArrayList<Endpoint> nodes, final int numStreams) {
		final Random random = new Random();
		final ArrayList<Endpoint> retval = new ArrayList<>();
		while (retval.size() < numStreams && nodes.size() > 0) {
			final int index = random.nextInt(nodes.size());
			final Endpoint node = nodes.remove(index);
			// TODO test that we can connect OK
			// TODO CLI static db, user, and pwd variables
			// TODO indicate what database we will be loading into
			// TODO and the userid and password to use for authentication to that database
			// TODO after connecting we need to store the conenctions somewhere
			// TODO so that they can be used later - a static collection is probably OK
			// TODO maybe a hashmap that's keyed by endpoint
			// if (!connectToIngressNode(node))
			// {
			// continue;
			// }

			retval.add(node);
		}

		return retval;
	}

	/*
	 * The actual work for bulk loading data from files into table via ingress
	 * Returns the number of rows loaded
	 */
	private static long doLoadFromFiles(final ArrayList<Path> files, final ArrayList<Endpoint> ingressNodes,
			final TableMetadata tableMetadata, final String delimiter) {
		// Start threads for communication with ingress nodes
		final ArrayList<IngressSourceThread> threads = new ArrayList<>();
		for (final Endpoint endpoint : ingressNodes) {
			final IngressSourceThread thread = new IngressSourceThread(endpoint, tableMetadata);
			threads.add(thread);
		}

		final CountDownLatch latch = new CountDownLatch(threads.size());
		for (final IngressSourceThread thread : threads) {
			thread.setAllThreads(threads);
			thread.setLatch(latch);
			thread.start();
		}

		// Start threads for reading files
		final ArrayList<FileReaderThread> threads2 = new ArrayList<>();
		for (final Path path : files) {
			final FileReaderThread thread = new FileReaderThread(path, tableMetadata, delimiter, threads);
			threads2.add(thread);
			thread.start();
		}

		// Join file readers
		long failedRows = 0;
		for (final FileReaderThread thread : threads2) {
			while (true) {
				try {
					thread.join();
					if (!thread.ok()) {
						thread.getException().printStackTrace();
						for (final IngressSourceThread thread2 : threads) {
							thread2.setAbort();
						}
					}

					failedRows += thread.getNumFailedRows();
					break;
				} catch (final InterruptedException e) {
				}
			}
		}

		// Send data end marker to each ingress source thread
		for (final IngressSourceThread thread : threads) {
			while (true) {
				try {
					thread.getQueue().put(new DataEndMarker());
					break;
				} catch (final InterruptedException e) {
				}
			}
		}

		long numRows = 0;
		boolean aborted = false;
		// Join comms threads
		for (final IngressSourceThread thread : threads) {
			while (true) {
				try {
					thread.join();
					if (thread.getAbort()) {
						aborted = true;
					}
					numRows += thread.getNumRowsLoaded();
					failedRows += thread.getNumFailedRows();
					break;
				} catch (final InterruptedException e) {
				}
			}
		}

		if (aborted) {
			return -1;
		}

		if (failedRows > 1) {
			System.out.println(failedRows + " rows failed to be loaded");
		} else if (failedRows == 1) {
			System.out.println("1 row failed to be loaded");
		}

		return numRows;
	}

	/*
	 * Convert a glob to a list of filenames
	 */
	private static ArrayList<Path> processGlob(final String glob) {
		final ArrayList<Path> files = new ArrayList<>();
		final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + glob);
		int a = 0;
		int b = 0;
		while (a < glob.length()) {
			if (glob.charAt(a) == '/') {
				b = a;
			}

			if (glob.charAt(a) == '*') {
				break;
			}

			a++;
		}

		final String startingPath = glob.substring(0, b + 1);
		final Set<FileVisitOption> options = new HashSet<>();
		final HashSet<String> dirs = new HashSet<>();
		options.add(FileVisitOption.FOLLOW_LINKS);

		try {
			Files.walkFileTree(Paths.get(startingPath), options, Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult postVisitDirectory(final Path file, final IOException exc) throws IOException {
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult visitFile(final Path file,
						final java.nio.file.attribute.BasicFileAttributes attrs) throws IOException {
					try {
						final String dir = file.getParent().toString();
						if (!dirs.contains(dir)) {
							dirs.add(dir);
						}
						if (matcher.matches(file)) {
							files.add(file);
						}
						return FileVisitResult.CONTINUE;
					} catch (final Exception e) {
						return FileVisitResult.CONTINUE;
					}
				}

				@Override
				public FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException {
					return FileVisitResult.CONTINUE;
				}
			});
		} catch (final Exception e) {
			e.printStackTrace();
			return files;
		}

		return files;
	}

	/*
	 * Contains the hostname and port on which we can connect to an ingress node
	 */
	private static class Endpoint {
		private final String host;
		private final int port;

		public Endpoint(String host, int port) {
			this.host = host;
			this.port = port;
		}

		@Override
		public boolean equals(final Object r) {
			if (r == null) {
				return false;
			}

			if (!(r instanceof Endpoint)) {
				return false;
			}

			final Endpoint rhs = (Endpoint) r;
			return host.equals(rhs.host) && port == rhs.port;
		}

		@Override
		public int hashCode() {
			int hash = 23;
			hash = hash * 31 + host.hashCode();
			hash = hash * 31 + port;
			return hash;
		}
	}

	/*
	 * Holds metadata for the table that we are going to load into The metadata is
	 * retrieved from an ingress server
	 */
	private static class TableMetadata {
		// TODO this will have to include the columns and their data types as well
		// TODO also needs to tell us what columns are in the hash an in what order
		// TODO also needs to tell us what the time column is
		private String fullyQualifiedName; // The fully qualified name of the table
		private int heartbeatInMs; // Ingress server is requesting heartbeat messages on this interval
		private int internalStreamsPerNode; // This is the number of internal streams per ingress node - we may not use
											// this right away but we should be fetching this info and filling it in

		public int getHeartbeat() {
			return heartbeatInMs;
		}
	}

	/*
	 * The thread responsible for reading a single input file
	 */
	private static class FileReaderThread extends Thread {
		/*
		 * Return the approx size in bytes for the row
		 */
		private static int approxSizeInBytes(final ArrayList<Object> row) {
			// TODO doesn't have to be exact
			// TODO just go through the objects in the list
			// TODO check their types and add a value that makes sense
			return 1;
		}

		private final Path file;
		private final TableMetadata meta;
		private final String delim;
		private final ArrayList<IngressSourceThread> threads;
		private boolean ok = true;
		private Exception e;
		private final long failedRows = 0;
		// Map from key (according to metadata) to a sorted bag of rows
		private final HashMap<ArrayList<Object>, TreeBag> buckets = new HashMap<>();
		// Using AtomicInteger so that we don't have to allocate a new object every time
		// we update
		private final HashMap<ArrayList<Object>, AtomicInteger> approxSizeByKey = new HashMap<>();
		private final TreeMultimap<Integer, ArrayList<Object>> keysByApproxSize;

		private int bufferedData = 0;

		public FileReaderThread(final Path file, final TableMetadata meta, final String delim,
				final ArrayList<IngressSourceThread> threads) {
			this.file = file;
			this.meta = meta;
			this.delim = delim;
			this.threads = threads;
			keysByApproxSize = TreeMultimap.create(new ReverseIntComparator(), new KeyComparator(meta));
		}

		public Exception getException() {
			return e;
		}

		public long getNumFailedRows() {
			return failedRows;
		}

		public boolean ok() {
			return ok;
		}

		@Override
		public void run() {
			try {
				final BufferedReader in = new BufferedReader(new FileReader(file.toFile()), 512 * 1024);
				String line = in.readLine();
				if (line == null) {
					in.close();
					return;
				}

				// For the first line, we'll do this twice, but it prevents an object allocation
				// in the loop
				final FastStringTokenizer fst = new FastStringTokenizer(line, delim, false);
				final ArrayList<Object> row = new ArrayList<>();
				final ArrayList<Object> key = new ArrayList<>();
				long rr = 0;
				while (line != null) {
					fst.reuse(line, delim, false);
					final String[] tokens = fst.allTokens();
					row.clear();
					key.clear();
					// TODO parse tokens into row based on table metadata
					// TODO if parsing of a row fails, don't consider that a failure
					// TODO instead just increment failed rows
					// TODO and I'd like to capture the data of the failed row
					// TODO and store it in a file for the user
					// TODO just like I'd like to do with failed rows coming back
					// TODO from the ingress server
					// TODO fill in key based on metadata
					TreeBag bag = buckets.get(key);
					if (bag == null) {
						bag = new TreeBag(new TimeColComparator(meta));
						buckets.put(key, bag);
					}

					bag.add(row);
					final int approx = approxSizeInBytes(row);
					bufferedData += approx;
					AtomicInteger size = approxSizeByKey.get(key);
					if (size == null) {
						size = new AtomicInteger(0);
						approxSizeByKey.put(key, size);
					}

					final int oldSize = size.get();
					final int newSize = size.addAndGet(approx);

					if (oldSize != 0) {
						keysByApproxSize.remove(oldSize, key);
					}
					keysByApproxSize.put(newSize, key);

					if (bag.size() >= BAG_FLUSH_SIZE) {
						// Round robin to IngressSourceThread
						buckets.remove(key);
						threads.get((int) (rr++ % threads.size())).getQueue().put(bag);

						size = approxSizeByKey.remove(key);
						final int s = size.get();
						keysByApproxSize.remove(size, key);
						bufferedData -= s;
					}

					while (bufferedData >= MAX_BAGS_SIZE_PER_READER) {
						// Flush largest bins till under limit
						for (final Map.Entry<Integer, ArrayList<Object>> entry : keysByApproxSize.entries()) {
							final int s = entry.getKey();
							final ArrayList<Object> key2 = entry.getValue();
							final TreeBag bag2 = buckets.remove(key2);
							threads.get((int) (rr++ % threads.size())).getQueue().put(bag2);

							approxSizeByKey.remove(key2);
							keysByApproxSize.remove(s, key2);
							bufferedData -= s;
							break;
						}
					}

					line = in.readLine();
				}

				in.close();
			} catch (final Exception e) {
				ok = false;
				this.e = e;
			}
		}
	}

	/*
	 * This is a thread that communicates with an ingress server
	 */
	private static class IngressSourceThread extends Thread {
		private final ArrayBlockingQueue<Object> in = new ArrayBlockingQueue<>(16); // Input queue to this thread
		private final long heartbeat;
		private final long loadedRows = 0;
		private final long failedRows = 0;
		private final AtomicBoolean abort = new AtomicBoolean(false);
		private CountDownLatch latch;

		public IngressSourceThread(final Endpoint node, final TableMetadata meta) {
			heartbeat = meta.getHeartbeat();
		}

		public boolean getAbort() {
			return abort.get();
		}

		public long getNumFailedRows() {
			return failedRows;
		}

		public long getNumRowsLoaded() {
			return loadedRows;
		}

		public ArrayBlockingQueue<Object> getQueue() {
			return in;
		}

		@Override
		public void run() {
			// TODO create a new session with the ingress server using existing connection
			// TODO send heartbeat message
			while (true) {
				if (abort.get()) {
					// TODO send abort session message to ingress server
					// TODO close connection
					return;
				}

				Object o = null;
				try {
					o = in.poll(heartbeat, TimeUnit.MILLISECONDS);
				} catch (final InterruptedException f) {
				}

				if (o == null) {
					// TODO send heartbeat
					// TODO if connection is lost, attempt to reconnect and then send heartbeat
					// TODO If reconnection fails
					// TODO send abort session message to ingress server
					// TODO Set abort flag on all IngressSourceThreads
					// TODO close connection
					// TODO return
				} else if (o instanceof DataEndMarker) // no more data
				{
					// Check again before entering latch
					if (abort.get()) {
						// TODO send abort session message to ingress server
						// TODO close connection
						return;
					}

					latch.countDown();
					while (true) {
						try {
							final boolean done = latch.await(heartbeat, TimeUnit.MILLISECONDS);
							if (done) {
								break;
							}
						} catch (final InterruptedException f) {
						}

						if (abort.get()) {
							// TODO send abort session message to ingress server
							// TODO close connection
							return;
						}

						// TODO send heartbeat
						// TODO if connection is lost, attempt to reconnect and then send heartbeat
						// TODO If reconnection fails
						// TODO send abort session message to ingress server
						// TODO Set abort flag on all IngressSourceThreads
						// TODO close connection
						// TODO return
					}

					// TODO tell ingress to do a full commit of the session (not partial like with
					// kafka)
					// TODO set number of failed rows (if any)
					// TODO subtract the number of failed rows from loadedRows
					// TODO I'd really like to be able to be able to get the actual failed rows and
					// TODO write them to a file so the caller can see what failed
					// TODO I've asked for it to be implemented by the ingress server
					// TODO but I don't yet know if that will make it into V1 or not

					// If we have a comms failure in the middle of trying to send
					// the commit, we are truly good and screwed for now
					// Cross your fingers and try to reconnect
					// If it fails set
					// failedRows to the current value of loadedRows
					// and set loadedRows to zero, close the connection, and return
					// In the future we want to implement ingress server 2PC across all open
					// sessions

					// TODO close this connection
					return;
				} else {
					// We have data to send to the ingress server - make sure to increment
					// loadedRows
					// TODO compute 32 bit FNV-1 hash for every row and send data
					// TODO if connection is lost, attempt to reconnect and then send heartbeat
					// TODO If reconnection fails
					// TODO send abort session message to ingress server
					// TODO Set abort flag on all IngressSourceThreads
					// TODO close connection
					// TODO return
				}
			}
		}

		public void setAbort() {
			abort.set(true);
		}

		public void setAllThreads(final ArrayList<IngressSourceThread> threads) {
		}

		public void setLatch(final CountDownLatch latch) {
			this.latch = latch;
		}
	}

	/*
	 * Given 2 ArrayList<Object>s containing just the keys of two rows, compare the
	 * keys
	 */
	private static class KeyComparator implements Comparator {
		public KeyComparator(final TableMetadata meta) {
		}

		@Override
		public int compare(final Object arg0, final Object arg1) {
			// TODO the metadata object is here if you want to use it
			// TODO both arraylist will have the same size and the same types in the same
			// positions
			// TODO just need to compare them
			return 0;
		}
	}

	/*
	 * Sort integer keys in descending order
	 */
	private static class ReverseIntComparator implements Comparator {
		@Override
		public int compare(final Object arg0, final Object arg1) {
			final Integer lhs = (Integer) arg0;
			final Integer rhs = (Integer) arg1;
			return -1 * lhs.compareTo(rhs);
		}

	}

	/*
	 * Sort rows by time column
	 */
	private static class TimeColComparator implements Comparator {
		public TimeColComparator(final TableMetadata meta) {
		}

		@Override
		public int compare(final Object arg0, final Object arg1) {
			// TODO need to use metadata to pull out the time column
			// TODO from the row and compare them
			// TODO return -1 if time col from left row is less than time col from right row
			// TODO return 1 if time col from left row is greater than time col from right
			// row
			// TODO otherwise return 0
			return 0;
		}
	}

//	/*
//	 * Calls nextToken on a tokenizer, but trims tokens and throws away empty ones
//	 */
//	private static String betterNextToken(final StringTokenizer tokens) throws NoSuchElementException {
//		String retval = "";
//		while (retval.equals(""))
//		{
//			retval = tokens.nextToken().trim();
//		}
//
//		return retval;
//	}

	private final static char[] hexArray = "0123456789abcdef".toCharArray();

	private static String bytesToHex(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}

	private static void printResultSet(final ResultSet rs, final ResultSetMetaData meta) throws Exception {
		if (!performance) {
			final ArrayList<Integer> offsets = new ArrayList<>();
			int i = 1;
			final StringBuilder line = new StringBuilder(64 * 1024);
			final int colCount = meta.getColumnCount();
			while (i <= colCount) {
				offsets.add(line.length());
				final int nameWidth = meta.getColumnName(i).length();
				final int valsWidth = meta.getColumnDisplaySize(i);
				line.append((meta.getColumnName(i)));
				int j = 0;
				while (j < Math.max((valsWidth - nameWidth), 1)) {
					line.append(" ");
					j++;
				}

				i++;
			}

			System.out.println(line);
			final int len = line.length();
			line.setLength(0);
			i = 0;
			while (i < len) {
				line.append('-');
				i++;
			}

			System.out.println(line);
			line.setLength(0);
			long rowCount = 0;
			while (rs.next()) {
				rowCount++;
				i = 1;
				final int s = line.length();
				while (i <= colCount) {
					final int target = s + offsets.get(i - 1);
					final int x = target - line.length();
					int y = 0;
					while (y < x) {
						line.append(" ");
						y++;
					}
					Object o = (rs.getObject(i));
					if (rs.wasNull()) {
						o = "NULL";
					} else if (o instanceof Time) {
						SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
						final TimeZone utc = TimeZone.getTimeZone("UTC");
						sdf.setTimeZone(utc);
						String timeStr = sdf.format(o);
						o = timeStr;
					} else if (o instanceof Date) {
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
						final TimeZone utc = TimeZone.getTimeZone("UTC");
						sdf.setTimeZone(utc);
						String dateStr = sdf.format(o);
						o = dateStr;
					} else if (o instanceof byte[]) {
						o = "0x" + bytesToHex((byte[]) o);
					}
					line.append(o);
					line.append(" ");
					i++;
				}

				if (line.length() >= 32 * 1024) {
					System.out.println(line);
					line.setLength(0);
				} else {
					line.append("\n");
				}
			}

			if (line.length() != 0) {
				System.out.println(line);
			}
			System.out.println("Fetched " + rowCount + (rowCount == 1 ? " row" : " rows"));
		} else {
			long rowCount = 0;
			while (rs.next()) {
				rowCount++;
			}

			System.out.println("Fetched " + rowCount + (rowCount == 1 ? " row" : " rows"));
		}
	}

	private static void outputResultSet(final ResultSet rs, final ResultSetMetaData meta) throws Exception {
		FileOutputStream out = new FileOutputStream(outputCSVFile);
		for (int i = 1; i <= meta.getColumnCount(); i++) {
			String colType = meta.getColumnTypeName(i);
			if (colType != null) {
				out.write(colType.getBytes());
			}
			if (i < meta.getColumnCount())
				out.write(',');
		}
		out.write('\n');
		for (int i = 1; i <= meta.getColumnCount(); i++) {
			String colType = meta.getColumnLabel(i);
			if (colType != null) {
				out.write(colType.getBytes());
			}
			if (i < meta.getColumnCount())
				out.write(',');
		}
		out.write('\n');
		int rowCount = 0;
		while (rs.next()) {
			for (int i = 1; i <= meta.getColumnCount(); i++) {
				out.write('"');
				Object o = rs.getObject(i);
				String valueString = "NULL";
				if (rs.wasNull()) {
					valueString = "NULL";
				} else if (o instanceof byte[]) {
					valueString = "0x" + bytesToHex((byte[]) o);
				} else if (o instanceof Time) {
					SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
					final TimeZone utc = TimeZone.getTimeZone("UTC");
					sdf.setTimeZone(utc);
					valueString = sdf.format(o);
				} else if (o instanceof Date) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
					final TimeZone utc = TimeZone.getTimeZone("UTC");
					sdf.setTimeZone(utc);
					valueString = sdf.format(o);
				} else if (o != null) {
					valueString = o.toString();
				}
				for (int j = 0; j < valueString.length(); j++) {
					if (valueString.charAt(j) == '"') {
						out.write("\"\"".getBytes());
					} else {
						out.write(valueString.charAt(j));
					}
				}
				out.write('"');
				if (i < meta.getColumnCount())
					out.write(',');
			}
			out.write('\n');
			rowCount++;
		}
		out.close();
		System.out.println("Fetched " + rowCount + (rowCount == 1 ? " row" : " rows"));
	}

	private static void printWarnings(final ResultSet rs) throws SQLException {
		SQLWarning warn = rs.getWarnings();
		while (warn != null) {
			System.out.println("Warning: " + warn.getMessage());
			warn = warn.getNextWarning();
		}
	}

	private static void printWarnings(final Statement st) throws SQLException {
		SQLWarning warn = st.getWarnings();
		while (warn != null) {
			System.out.println("Warning: " + warn.getMessage());
			warn = warn.getNextWarning();
		}
	}

	private static void printTime(long start, long end) {
		if (timing) {
			System.out.println("\nCommand took " + (end - start) / 1000.0 + " seconds");
		}
	}

	/*
	 * unused?
	 * 
	 * We have a one or two part table name that might be double quoted with unknown
	 * case We want the actual name of the table being referenced
	 */
	private static String properTable(final String table) throws Exception {
		final StringTokenizer tokens = new StringTokenizer(table, ".\"", true);
		String token = tokens.nextToken();
		if (token.equals("\"")) {
			// It's either a quoted table name or schema name
			String name = tokens.nextToken();
			if (!tokens.nextToken().equals("\"")) {
				throw new Exception(); // This will trigger syntax error message
			}

			if (tokens.hasMoreTokens()) {
				if (!tokens.nextToken().equals(".")) {
					throw new Exception();
				}

				token = tokens.nextToken();
				if (token.equals("\"")) {
					// name is the schema name
					// This is a quoted table name
					final String schema = name;
					name = tokens.nextToken();
					if (!tokens.nextToken().equals("\"")) {
						throw new Exception();
					}

					if (tokens.hasMoreTokens()) {
						throw new Exception();
					}

					return schema + "." + name;
				} else {
					// name is the schema name
					// This is an unquoted table name
					final String schema = name;
					name = tokens.nextToken().toLowerCase();
					if (tokens.hasMoreTokens()) {
						throw new Exception();
					}

					return schema + "." + name;
				}
			} else {
				// name is the table name
				// We have to get the schema name from the JDBC driver
				final String schema = conn.getSchema();
				return schema + "." + name;
			}
		} else {
			// Unquoted schema or table name
			String name = token.toLowerCase();

			if (tokens.hasMoreTokens()) {
				if (!tokens.nextToken().equals(".")) {
					throw new Exception();
				}

				token = tokens.nextToken();
				if (token.equals("\"")) {
					// name is the schema name
					// This is a quoted table name
					final String schema = name;
					name = tokens.nextToken();
					if (!tokens.nextToken().equals("\"")) {
						throw new Exception();
					}

					if (tokens.hasMoreTokens()) {
						throw new Exception();
					}

					return schema + "." + name;
				} else {
					// name is the schema name
					// This is an unquoted table name
					final String schema = name;
					name = tokens.nextToken().toLowerCase();
					if (tokens.hasMoreTokens()) {
						throw new Exception();
					}

					return schema + "." + name;
				}
			} else {
				// name is the table name
				// We have to get the schema name from the JDBC driver
				final String schema = conn.getSchema();
				return schema + "." + name;
			}
		}
	}
}
