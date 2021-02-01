package com.ocient.jdbc;

import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class XGDate extends Date
{

	/** */
	private static final long serialVersionUID = 4666058265622281223L;

	public static Date from(final Instant instant)
	{
		return new XGDate(instant.toEpochMilli());
	}

	@Deprecated
	public static long parse(final String s)
	{
		try
		{
			final DateFormat format = DateFormat.getDateInstance();
			format.setTimeZone(TimeZone.getDefault());
			final GregorianCalendar cal = new GregorianCalendar();
			cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
			cal.setTimeZone(TimeZone.getDefault());
			format.setCalendar(cal);
			final java.util.Date date = format.parse(s);
			return date.getTime();
		}
		catch (final Exception e)
		{
			return 0;
		}
	}

	@Deprecated
	public static long UTC(final int year, final int month, final int date, final int hrs, final int min, final int sec)
	{
		final GregorianCalendar cal = new GregorianCalendar(year + 1900, month, date, hrs, min, sec);
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		return cal.getTime().getTime();
	}

	public static Date valueOf(final LocalDate date)
	{
		return new XGDate(java.sql.Date.valueOf(date));
	}

	public static Date valueOf(final String s)
	{
		return new XGDate(java.sql.Date.valueOf(s));
	}

	public XGDate(final Date date)
	{
		super(date.getTime());
	}

	@Deprecated
	public XGDate(final int year, final int month, final int day)
	{
		super(UTC(year, month, day, 0, 0, 0));
	}

	public XGDate(final long date)
	{
		super(date);
	}

	public XGDate(final XGTimestamp ts)
	{
		super(ts.getTime());
	}

	@Override
	public boolean equals(final Object obj)
	{
		if (obj instanceof java.util.Date)
		{
			return getTime() == ((java.util.Date) obj).getTime();
		}

		return false;
	}

	@Override
	@Deprecated
	public int getDate()
	{
		final SimpleDateFormat sdf = new SimpleDateFormat("d");
		sdf.setTimeZone(TimeZone.getDefault());
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getDefault());
		sdf.setCalendar(cal);
		return Integer.parseInt(sdf.format(this));
	}

	@Override
	@Deprecated
	public int getDay()
	{
		final SimpleDateFormat sdf = new SimpleDateFormat("u");
		sdf.setTimeZone(TimeZone.getDefault());
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getDefault());
		sdf.setCalendar(cal);
		int retval = Integer.parseInt(sdf.format(this));
		if (retval == 7)
		{
			retval = 0;
		}

		return retval;
	}

	@Override
	@Deprecated
	public int getMonth()
	{
		final SimpleDateFormat sdf = new SimpleDateFormat("M");
		sdf.setTimeZone(TimeZone.getDefault());
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getDefault());
		sdf.setCalendar(cal);
		return Integer.parseInt(sdf.format(this)) - 1;
	}

	@Override
	@Deprecated
	public int getTimezoneOffset()
	{
		return (int) ((getTime() - UTC(getYear(), getMonth(), getDate(), getHours(), getMinutes(), getSeconds())) / (60 * 1000));
	}

	@Override
	@Deprecated
	public int getYear()
	{
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
		sdf.setTimeZone(TimeZone.getDefault());
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getDefault());
		sdf.setCalendar(cal);
		return Integer.parseInt(sdf.format(this)) - 1900;
	}

	@Override
	@Deprecated
	public void setDate(final int date)
	{
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		sdf.setCalendar(cal);
		final String current = sdf.format(this);
		final String newVal = current.substring(0, 8) + String.format("%02d", date) + current.substring(10);
		setTime(parse(newVal));
	}

	@Override
	@Deprecated
	public void setMonth(final int month)
	{
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		sdf.setCalendar(cal);
		final String current = sdf.format(this);
		final String newVal = current.substring(0, 5) + String.format("%02d", month + 1) + current.substring(7);
		setTime(parse(newVal));
	}

	@Override
	@Deprecated
	public void setYear(final int year)
	{
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		sdf.setCalendar(cal);
		final String current = sdf.format(this);
		final String newVal = String.format("%04d", year + 1900) + current.substring(4);
		setTime(parse(newVal));
	}

	@Override
	@Deprecated
	public String toGMTString()
	{
		final SimpleDateFormat sdf = new SimpleDateFormat("d MMM yyyy HH:mm:ss");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		sdf.setCalendar(cal);
		return sdf.format(this) + " GMT";
	}

	@Override
	public Instant toInstant() throws UnsupportedOperationException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public LocalDate toLocalDate()
	{
		return LocalDate.of(getYear() + 1900, getMonth() + 1, getDate());
	}

	@Override
	@Deprecated
	public String toLocaleString()
	{
		final DateFormat format = DateFormat.getDateInstance();
		format.setTimeZone(TimeZone.getDefault());
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getDefault());
		format.setCalendar(cal);
		return format.format(this);
	}

	// Always returns //UTC string
	@Override
	public String toString()
	{
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		sdf.setCalendar(cal);
		return sdf.format(this);
	}
}
