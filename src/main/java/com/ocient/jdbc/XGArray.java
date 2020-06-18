package com.ocient.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

public class XGArray implements java.sql.Array {
	private byte type;
	private Object[] array;
	private XGConnection conn;
	private XGStatement stmt;

	public XGArray(int numElements, byte type, final XGConnection conn, final XGStatement stmt) {
		this.type = type;
		this.conn = conn;
		this.stmt = stmt;
		array = new Object[numElements];
	}

	@Override
	public void free() throws SQLException {
		array = new Object[0];
	}

	@Override
	public Object getArray() throws SQLException {
		return array;
	}

	@Override
	public Object getArray(Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getArray(long index, int count) throws SQLException {
		try {
			return Arrays.copyOfRange(array, (int) index - 1, (int) index + count - 1);
		} catch (Exception e) {
			throw SQLStates.newGenericException(e);
		}
	}

	@Override
	public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getBaseType() throws SQLException {
		switch (type) {
		case 1:
			return Types.INTEGER;
		case 2:
			return Types.BIGINT;
		case 3:
			return Types.FLOAT;
		case 4:
			return Types.DOUBLE;
		case 5:
			return Types.VARCHAR;
		case 6:
			return Types.TIMESTAMP;
		case 7:
			return Types.NULL;
		case 8:
			return Types.BOOLEAN;
		case 9:
			return Types.VARBINARY;
		case 10:
			return Types.TINYINT;
		case 11:
			return Types.SMALLINT;
		case 12:
			return Types.TIME;
		case 13:
			return Types.DECIMAL;
		case 14:
			return Types.ARRAY;
		case 15:
		case 16:
		case 17:
		case 18:
			return Types.OTHER;
		case 19:
			return Types.DATE;
		default:
			throw SQLStates.INVALID_COLUMN_TYPE.clone();
		}
	}

	@Override
	public String getBaseTypeName() throws SQLException {
		switch (type) {
		case 1:
			return "INTEGER";
		case 2:
			return "BIGINT";
		case 3:
			return "FLOAT";
		case 4:
			return "DOUBLE";
		case 5:
			return "VARCHAR";
		case 6:
			return "TIMESTAMP";
		case 7:
			return "NULL";
		case 8:
			return "BOOLEAN";
		case 9:
			return "VARBINARY";
		case 10:
			return "BYTE";
		case 11:
			return "SMALLINT";
		case 12:
			return "TIME";
		case 13:
			return "DECIMAL";
		case 14:
			return "ARRAY";
		case 15:
			return "UUID";
		case 16:
			return "ST_POINT";
		case 17:
			return "IP";
		case 18:
			return "IPV4";
		case 19:
			return "DATE";
		default:
			throw SQLStates.INVALID_COLUMN_TYPE.clone();
		}
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		ArrayList<Object> alo = new ArrayList<Object>();
		int i = 1;
		for (Object o : array) {
			ArrayList<Object> row = new ArrayList<Object>();
			row.add(i++);
			row.add(o);
			alo.add(row);
		}

		XGResultSet retval = new XGResultSet(conn, alo, stmt);
		Map<String, Integer> cols2Pos = new HashMap<String, Integer>();
		TreeMap<Integer, String> pos2Cols = new TreeMap<Integer, String>();
		Map<String, String> cols2Types = new HashMap<String, String>();
		cols2Pos.put("index", 0);
		cols2Pos.put("array_value", 1);
		pos2Cols.put(0, "index");
		pos2Cols.put(1, "array_value");
		cols2Types.put("index", "INT");

		switch (type) {
		case 1:
			cols2Types.put("array_value", "INT");
			break;
		case 2:
			cols2Types.put("array_value", "LONG");
			break;
		case 3:
			cols2Types.put("array_value", "FLOAT");
			break;
		case 4:
			cols2Types.put("array_value", "DOUBLE");
			break;
		case 5:
			cols2Types.put("array_value", "CHAR");
			break;
		case 6:
			cols2Types.put("array_value", "TIMESTAMP");
			break;
		case 7:
			cols2Types.put("array_value", "NULL");
			break;
		case 8:
			cols2Types.put("array_value", "BOOLEAN");
			break;
		case 9:
			cols2Types.put("array_value", "BINARY");
			break;
		case 10:
			cols2Types.put("array_value", "BYTE");
			break;
		case 11:
			cols2Types.put("array_value", "SHORT");
			break;
		case 12:
			cols2Types.put("array_value", "TIME");
			break;
		case 13:
			cols2Types.put("array_value", "DECIMAL");
			break;
		case 14:
			cols2Types.put("array_value", "ARRAY");
			break;
		case 15:
			cols2Types.put("array_value", "UUID");
			break;
		case 16:
			cols2Types.put("array_value", "ST_POINT");
			break;
		case 17:
			cols2Types.put("array_value", "IP");
			break;
		case 18:
			cols2Types.put("array_value", "IPV4");
			break;
		case 19:
			cols2Types.put("array_value", "DATE");
			break;
		default:
			throw SQLStates.INVALID_COLUMN_TYPE.clone();
		}

		retval.setCols2Pos(cols2Pos);
		retval.setPos2Cols(pos2Cols);
		retval.setCols2Types(cols2Types);
		return retval;
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getResultSet(long index, int count) throws SQLException {
		ArrayList<Object> alo = new ArrayList<Object>();
		for (int i = (int) index; i < index + count; i++) {
			ArrayList<Object> row = new ArrayList<Object>();
			row.add(i);
			row.add(array[i]);
			alo.add(row);
		}

		XGResultSet retval = new XGResultSet(conn, alo, stmt);
		Map<String, Integer> cols2Pos = new HashMap<String, Integer>();
		TreeMap<Integer, String> pos2Cols = new TreeMap<Integer, String>();
		Map<String, String> cols2Types = new HashMap<String, String>();
		cols2Pos.put("index", 0);
		cols2Pos.put("array_value", 1);
		pos2Cols.put(0, "index");
		pos2Cols.put(1, "array_value");
		cols2Types.put("index", "INT");

		switch (type) {
		case 1:
			cols2Types.put("array_value", "INT");
			break;
		case 2:
			cols2Types.put("array_value", "LONG");
			break;
		case 3:
			cols2Types.put("array_value", "FLOAT");
			break;
		case 4:
			cols2Types.put("array_value", "DOUBLE");
			break;
		case 5:
			cols2Types.put("array_value", "CHAR");
			break;
		case 6:
			cols2Types.put("array_value", "TIMESTAMP");
			break;
		case 7:
			cols2Types.put("array_value", "NULL");
			break;
		case 8:
			cols2Types.put("array_value", "BOOLEAN");
			break;
		case 9:
			cols2Types.put("array_value", "BINARY");
			break;
		case 10:
			cols2Types.put("array_value", "BYTE");
			break;
		case 11:
			cols2Types.put("array_value", "SHORT");
			break;
		case 12:
			cols2Types.put("array_value", "TIME");
			break;
		case 13:
			cols2Types.put("array_value", "DECIMAL");
			break;
		case 14:
			cols2Types.put("array_value", "ARRAY");
			break;
		case 15:
			cols2Types.put("array_value", "UUID");
			break;
		case 16:
			cols2Types.put("array_value", "ST_POINT");
			break;
		case 17:
			cols2Types.put("array_value", "IP");
			break;
		case 18:
			cols2Types.put("array_value", "IPV4");
			break;
		case 19:
			cols2Types.put("array_value", "DATE");
			break;
		default:
			throw SQLStates.INVALID_COLUMN_TYPE.clone();
		}

		retval.setCols2Pos(cols2Pos);
		retval.setPos2Cols(pos2Cols);
		retval.setCols2Types(cols2Types);
		return retval;
	}

	@Override
	public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void add(Object obj, int pos) {
		array[pos] = obj;
	}

	public String toString() {
		try {
			StringBuilder str = new StringBuilder();
			str.append("[");

			if (array.length > 0) {
				Object o = array[0];
				if (o == null) {
					str.append("NULL");
				} else if (getBaseType() == java.sql.Types.ARRAY) {
					str.append(((XGArray) o).toString());
				} else if (getBaseType() == java.sql.Types.TIME) {
					SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
					final TimeZone utc = TimeZone.getTimeZone("UTC");
					sdf.setTimeZone(utc);
					str.append(sdf.format(o));
				} else if (getBaseType() == java.sql.Types.TIMESTAMP) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
					final TimeZone utc = TimeZone.getTimeZone("UTC");
					sdf.setTimeZone(utc);
					str.append(sdf.format(o));
				} else if (getBaseType() == java.sql.Types.DATE) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					final TimeZone utc = TimeZone.getTimeZone("UTC");
					sdf.setTimeZone(utc);
					str.append(sdf.format(o));
				} else if (getBaseType() == java.sql.Types.BINARY || getBaseType() == java.sql.Types.VARBINARY) {
					str.append("0x" + bytesToHex((byte[]) o));
				} else {
					str.append(o);
				}
			}

			for (int i = 1; i < array.length; i++) {
				str.append(", ");
				Object o = array[i];
				if (o == null) {
					str.append("NULL");
				} else if (getBaseType() == java.sql.Types.ARRAY) {
					str.append(((XGArray) o).toString());
				} else if (getBaseType() == java.sql.Types.TIME) {
					SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
					final TimeZone utc = TimeZone.getTimeZone("UTC");
					sdf.setTimeZone(utc);
					str.append(sdf.format(o));
				} else if (getBaseType() == java.sql.Types.TIMESTAMP) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
					final TimeZone utc = TimeZone.getTimeZone("UTC");
					sdf.setTimeZone(utc);
					str.append(sdf.format(o));
				} else if (getBaseType() == java.sql.Types.DATE) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					final TimeZone utc = TimeZone.getTimeZone("UTC");
					sdf.setTimeZone(utc);
					str.append(sdf.format(o));
				} else if (getBaseType() == java.sql.Types.BINARY || getBaseType() == java.sql.Types.VARBINARY) {
					str.append("0x" + bytesToHex((byte[]) o));
				} else {
					str.append(o);
				}
			}

			str.append("]");
			return str.toString();
		} catch (Exception e) {
			return "Exception occurred accessing Array";
		}
	}

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
}
