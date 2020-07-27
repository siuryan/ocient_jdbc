package com.ocient.jdbc;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class XGTimestamp extends Timestamp {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8251859415809458619L;

	@Deprecated
	public XGTimestamp(int year, int month, int date, int hour, int minute, int second, int nano)
	{
		super(UTC(year, month, date, hour, minute, second));
		setNanos(nano);
	}
	
	public XGTimestamp(long time)
	{
		super(time);
	}
	
	public XGTimestamp(Timestamp ts)
	{
		super(ts.getTime());
		setNanos(ts.getNanos());
	}
	
	public XGTimestamp(XGDate date)
	{
		super(date.getTime());
	}
	
	public XGTimestamp addMs(int ms)
	{
		int fractionPastMs = getNanos() - (getNanos() / 1000000) * 1000000;
		XGTimestamp retval = new XGTimestamp(getTime() + ms);
		retval.setNanos(retval.getNanos() + fractionPastMs);
		return retval;
	}
	
	public static Timestamp valueOf(String s)
	{
		return new XGTimestamp(java.sql.Timestamp.valueOf(s));
	}
	
	public static Timestamp valueOf(LocalDateTime date)
	{
		return new XGTimestamp(java.sql.Timestamp.valueOf(date));
	}
	
	//Always returns //UTC string
	public String toString()
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		sdf.setCalendar(cal);
		return sdf.format(this) + "." + String.format("%09d", getNanos());
	}
	
	@Deprecated
	public static long UTC(int year, int month, int date, int hrs, int min, int sec)
	{
		GregorianCalendar cal = new GregorianCalendar(year + 1900, month, date, hrs, min, sec);
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		return cal.getTime().getTime();
	}
	
	@Deprecated
	public static long parse(String s)
	{
		try
		{
			DateFormat format = DateFormat.getDateInstance();
			format.setTimeZone(TimeZone.getDefault());
			GregorianCalendar cal = new GregorianCalendar();
			cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
			cal.setTimeZone(TimeZone.getDefault());
			format.setCalendar(cal);
			java.util.Date date = format.parse(s);
			return date.getTime();
		}
		catch(Exception e)
		{
			return 0;
		}
	}
	
	@Deprecated
	public int getYear()
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
		sdf.setTimeZone(TimeZone.getDefault());
		GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getDefault());
		sdf.setCalendar(cal);
		return Integer.parseInt(sdf.format(this)) - 1900;
	}
	
	@Deprecated
	public void setYear(int year)
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		sdf.setCalendar(cal);
		String current = sdf.format(this);
		String newVal = String.format("%04d", year + 1900) + current.substring(4);
		this.setTime(parse(newVal));
	}
	
	@Deprecated
	public int getMonth()
	{
		SimpleDateFormat sdf = new SimpleDateFormat("M");
		sdf.setTimeZone(TimeZone.getDefault());
		GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getDefault());
		sdf.setCalendar(cal);
		return Integer.parseInt(sdf.format(this)) - 1;
	}
	
	@Deprecated
	public void setMonth(int month)
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		sdf.setCalendar(cal);
		String current = sdf.format(this);
		String newVal = current.substring(0, 5) + String.format("%02d", month + 1) + current.substring(7);
		this.setTime(parse(newVal));
	}
	
	@Deprecated
	public int getDate()
	{
		SimpleDateFormat sdf = new SimpleDateFormat("d");
		sdf.setTimeZone(TimeZone.getDefault());
		GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getDefault());
		sdf.setCalendar(cal);
		return Integer.parseInt(sdf.format(this));
	}
	
	@Deprecated
	public void setDate(int date)
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		sdf.setCalendar(cal);
		String current = sdf.format(this);
		String newVal = current.substring(0, 8) + String.format("%02d", date) + current.substring(10);
		this.setTime(parse(newVal));
	}
	
	@Deprecated
	public int getDay()
	{
		SimpleDateFormat sdf = new SimpleDateFormat("u");
		sdf.setTimeZone(TimeZone.getDefault());
		GregorianCalendar cal = new GregorianCalendar();
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
	
	public boolean equals(Object obj)
	{
		if (obj instanceof java.sql.Timestamp)
		{
			return getTime() == ((java.sql.Timestamp)obj).getTime() &&
					getNanos() == ((java.sql.Timestamp)obj).getNanos();
		}
		
		if (obj instanceof java.util.Date)
		{
			return getTime() == ((java.util.Date)obj).getTime();
		}
		
		return false;
	}
	
	public boolean equals(Timestamp obj)
	{
		if (obj instanceof java.sql.Timestamp)
		{
			return getTime() == ((java.sql.Timestamp)obj).getTime() &&
					getNanos() == ((java.sql.Timestamp)obj).getNanos();
		}
		
		return false;
	}
	
	@Deprecated
	public String toLocaleString()
	{
		DateFormat format = DateFormat.getDateInstance();
		format.setTimeZone(TimeZone.getDefault());
		GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getDefault());
		format.setCalendar(cal);
		return format.format(this);
	}
	
	@Deprecated
	public String toGMTString()
	{
		SimpleDateFormat sdf = new SimpleDateFormat("d MMM yyyy HH:mm:ss");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		sdf.setCalendar(cal);
		return sdf.format(this) + " GMT";
	}
	
	@Deprecated
	public int getTimezoneOffset()
	{
		return (int)((this.getTime() - UTC(this.getYear(),
                this.getMonth(),
                this.getDate(),
                this.getHours(),
                this.getMinutes(),
                this.getSeconds())) / (60 * 1000));
	}
	
	public static Timestamp from(Instant instant)
	{
		XGTimestamp retval = new XGTimestamp(instant.toEpochMilli());
		retval.setNanos(instant.getNano());
		return retval;
	}
	
	public LocalDateTime toLocalDateTime()
	{
		return LocalDateTime.of(getYear() + 1900, getMonth() + 1, getDate(), getHours(), getMinutes(), getSeconds(), getNanos());
	}
	
	public Instant toInstant() 
	{
		Instant retval = Instant.ofEpochSecond(getTime() / 1000);
		retval.plusNanos(getNanos());
		return retval;
	}

}
