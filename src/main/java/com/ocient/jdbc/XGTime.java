package com.ocient.jdbc;

import java.sql.Time;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class XGTime extends Time {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6636929210798184587L;

	@Deprecated
	public XGTime(int hour, int minute, int second)
	{
		super(hour, minute, second);
	}

	public XGTime(long time)
	{
		super(time);
	}
	
	public XGTime(Time t)
	{
		super(t.getTime());
	}
	
	public static Time valueOf(String s)
	{
		return new XGTime(java.sql.Time.valueOf(s));
	}
	
	public XGTime addMs(int ms)
	{
		return new XGTime(getTime() + ms);
	}
	
	//Always returns //UTC string
	public String toString()
	{
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		sdf.setCalendar(cal);
		return sdf.format(this);
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
		if (obj instanceof java.util.Date)
		{
			return getTime() == ((java.util.Date)obj).getTime();
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
	
	public static Date from(Instant instant)
	{
		return new XGDate(instant.toEpochMilli());
	}
}
