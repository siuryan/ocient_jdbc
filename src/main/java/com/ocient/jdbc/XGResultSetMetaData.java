package com.ocient.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.TreeMap;
<<<<<<< HEAD
=======
import java.math.BigDecimal;
>>>>>>> 224b50a7964b25942082ec6b8e612ce43f0c62a7

public class XGResultSetMetaData implements ResultSetMetaData {
	public final Map<String, Integer> cols2Pos;
	public final TreeMap<Integer, String> pos2Cols;
	public final Map<String, String> cols2Types;

	public XGResultSetMetaData(final Map<String, Integer> cols2Pos2, final TreeMap<Integer, String> pos2Cols,
			final Map<String, String> cols2Types2) {
		this.cols2Pos = cols2Pos2;
		this.pos2Cols = pos2Cols;
		this.cols2Types = cols2Types2;
	}

	@Override
	public String getCatalogName(final int column) throws SQLException {
		return "";
	}

	@Override
	public String getColumnClassName(final int column) throws SQLException {
		final String name = pos2Cols.get(column - 1);
		final String type = cols2Types.get(name);

		if (type.equals("CHAR")) {
			return "java.lang.String";
		} else if (type.equals("BYTE")) {
			return "java.lang.Byte";
		} else if (type.equals("SHORT")) {
			return "java.lang.Short";
		} else if (type.equals("INT")) {
			return "java.lang.Integer";
		} else if (type.equals("FLOAT")) {
			return "java.lang.Float";
		} else if (type.equals("DOUBLE")) {
			return "java.lang.Double";
		} else if (type.equals("LONG")) {
			return "java.lang.Long";
		} else if (type.equals("TIMESTAMP")) {
			return "java.sql.Timestamp";
		} else if (type.equals("TIME")) {
			return "java.sql.Time";
		} else if (type.equals("DATE")) {
			return "java.util.Date";
		} else if (type.equals("BOOLEAN")) {
			return "java.lang.Boolean";
		} else if (type.equals("DECIMAL")) {
			return "java.math.BigDecimal";
		} else if (type.equals("BINARY")) {
			return "[B";
		} else {
			throw SQLStates.UNKNOWN_DATA_TYPE.clone();
		}
	}

	@Override
	public int getColumnCount() throws SQLException {
		return cols2Pos.size();
	}

	@Override
	public int getColumnDisplaySize(final int column) throws SQLException {
		final String name = pos2Cols.get(column - 1);
		final String type = cols2Types.get(name);
		int retval = 0;

		if (type.equals("BYTE")) {
			retval = 4;
		} else if (type.equals("SHORT")) {
			retval = 6;
		} else if (type.equals("INT")) {
			retval = 11;
		} else if (type.equals("LONG")) {
			retval = 20;
		} else if (type.equals("FLOAT")) {
			retval = 14;
		} else if (type.equals("DOUBLE")) {
			retval = 22;
		} else if (type.equals("TIMESTAMP")) {
			retval = 29;
		} else if (type.equals("TIME")) {
			retval = 13; // HH:MM:SS.mmm
		} else if (type.equals("DATE")) {
			retval = 11;
		} else if (type.equals("CHAR")) {
			retval = 45;
		} else if (type.equals("BOOLEAN")) {
			retval = 6;
		} else if (type.equals("BINARY")) {
			retval = 33;
		} else if (type.equals("DECIMAL")) {
			retval = 33; // max precision + '.' + "-"
		} else {
			throw SQLStates.UNKNOWN_DATA_TYPE.clone();
		}

		if (retval < name.length()) {
			retval = name.length();
		}

		return retval;
	}

	@Override
	public String getColumnLabel(final int column) throws SQLException {
		return pos2Cols.get(column - 1);
	}

	@Override
	public String getColumnName(final int column) throws SQLException {
		return pos2Cols.get(column - 1);
	}

	@Override
	public int getColumnType(final int column) throws SQLException {
		final String name = pos2Cols.get(column - 1);
		final String type = cols2Types.get(name);

		if (type.equals("CHAR")) {
			return java.sql.Types.VARCHAR;
		} else if (type.equals("BYTE")) {
			return java.sql.Types.TINYINT;
		} else if (type.equals("SHORT")) {
			return java.sql.Types.SMALLINT;
		} else if (type.equals("INT")) {
			return java.sql.Types.INTEGER;
		} else if (type.equals("FLOAT")) {
			return java.sql.Types.FLOAT;
		} else if (type.equals("DOUBLE")) {
			return java.sql.Types.DOUBLE;
		} else if (type.equals("LONG")) {
			return java.sql.Types.BIGINT;
		} else if (type.equals("TIMESTAMP")) {
			return java.sql.Types.TIMESTAMP;
		} else if (type.equals("TIME")) {
			return java.sql.Types.TIME;
		} else if (type.equals("DATE")) {
			return java.sql.Types.DATE;
		} else if (type.equals("BOOLEAN")) {
			return java.sql.Types.BOOLEAN;
		} else if (type.equals("BINARY")) {
			return java.sql.Types.BINARY;
		} else if (type.equals("DECIMAL")) {
			return java.sql.Types.DECIMAL;
		} else {
			throw SQLStates.UNKNOWN_DATA_TYPE.clone();
		}
	}

	@Override
	public String getColumnTypeName(final int column) throws SQLException {
		return cols2Types.get(pos2Cols.get(column - 1));
	}

	public int getPosition(final String name) throws Exception {
		return cols2Pos.get(name);
	}

	@Override
	public int getPrecision(final int column) throws SQLException {
		final String name = pos2Cols.get(column - 1);
		final String type = cols2Types.get(name);

		if (type.equals("CHAR")) {
			return 128 * 1024;
		} else if (type.equals("BYTE")) {
			return 3;
		} else if (type.equals("SHORT")) {
			return 5;
		} else if (type.equals("INT")) {
			return 10;
		} else if (type.equals("FLOAT")) {
			return 7;
		} else if (type.equals("DOUBLE")) {
			return 16;
		} else if (type.equals("LONG")) {
			return 20;
		} else if (type.equals("TIMESTAMP")) {
			return 23;
		} else if (type.equals("TIME")) {
			return 12;
		} else if (type.equals("DATE")) {
			return 10;
		} else if (type.equals("BOOLEAN")) {
			return 0;
		} else if (type.equals("BINARY")) {
			return 128 * 1024;
		} else if (type.equals("DECIMAL")) {
			return 31;
		} else {
			throw SQLStates.UNKNOWN_DATA_TYPE.clone();
		}
	}

	@Override
	public int getScale(final int column) throws SQLException {
		return 0;
	}

	@Override
	public String getSchemaName(final int column) throws SQLException {
		return "";
	}

	@Override
	public String getTableName(final int column) throws SQLException {
		return "";
	}

	@Override
	public boolean isAutoIncrement(final int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isCaseSensitive(final int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isCurrency(final int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(final int column) throws SQLException {
		return false;
	}

	@Override
	public int isNullable(final int column) throws SQLException {
		return ResultSetMetaData.columnNullableUnknown;
	}

	@Override
	public boolean isReadOnly(final int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isSearchable(final int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isSigned(final int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isWrapperFor(final Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public boolean isWritable(final int column) throws SQLException {
		return false;
	}

	@Override
	public String toString() {
		return "Position -> column = " + pos2Cols + "\nColumn -> type = " + cols2Types;
	}

	@Override
	public <T> T unwrap(final Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
