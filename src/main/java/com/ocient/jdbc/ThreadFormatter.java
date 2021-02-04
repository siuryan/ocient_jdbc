package com.ocient.jdbc;

import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class ThreadFormatter extends Formatter
{
	// Jun 11, 2020 9:46:15 PM com.ocient.jdbc.JDBCDriver createConnection
	// INFO: message
	private static final MessageFormat messageFormat = new MessageFormat("{3,date,hh:mm:ss} [{2}] {0} {5} {1}: {4} \n");

	public ThreadFormatter()
	{
	}

	@Override
	public String format(final LogRecord record)
	{
		final Object[] arguments = new Object[6];
		arguments[0] = record.getSourceClassName();
		arguments[1] = record.getLevel();
		arguments[2] = record.getThreadID();
		arguments[3] = new Date(record.getMillis());
		arguments[4] = record.getMessage();
		arguments[5] = record.getSourceMethodName();
		return messageFormat.format(arguments);
	}
}
