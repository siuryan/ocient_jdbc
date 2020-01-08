package com.ocient.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;
import com.ocient.jdbc.proto.ClientWireProtocol;
import com.ocient.jdbc.proto.ClientWireProtocol.CloseResultSet;
import com.ocient.jdbc.proto.ClientWireProtocol.ConfirmationResponse;
import com.ocient.jdbc.proto.ClientWireProtocol.ConfirmationResponse.ResponseType;
import com.ocient.jdbc.proto.ClientWireProtocol.FetchData;
import com.ocient.jdbc.proto.ClientWireProtocol.FetchMetadata;
import com.ocient.jdbc.proto.ClientWireProtocol.Request;

public class XGResultSet implements ResultSet {
	private static int bytesToInt(final byte[] val) {
		final int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
	}

	private static byte[] intToBytes(final int val) {
		final byte[] buff = new byte[4];
		buff[0] = (byte) (val >> 24);
		buff[1] = (byte) ((val & 0x00FF0000) >> 16);
		buff[2] = (byte) ((val & 0x0000FF00) >> 8);
		buff[3] = (byte) ((val & 0x000000FF));
		return buff;
	}

	private ArrayList<Object> rs = new ArrayList<>();
	private long firstRowIs = 0;
	private long position = -1;
	private boolean closed = false;
	private final XGConnection conn;
	private int fetchSize;
	private boolean wasNull = false;
	private Map<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Cols;
	private Map<String, String> cols2Types;
	private String stringData;

	//tell whether the resultset was constructed with a pre-defined dataset.    
	private boolean immutable = false;	

	private final XGStatement stmt;

	private final ArrayList<SQLWarning> warnings = new ArrayList<>();

	public XGResultSet(final XGConnection conn, final ArrayList<Object> rs, final XGStatement stmt)
	{
		this.conn = conn;
		this.rs = rs;
		this.stmt = stmt;
		this.rs.add(new DataEndMarker());
		this.immutable = true;
	}	

	public XGResultSet(final XGConnection conn, final int fetchSize, final XGStatement stmt) throws Exception {
		this.conn = conn;
		this.fetchSize = fetchSize;
		this.stmt = stmt;
		requestMetaData();
	}

	public XGResultSet(final XGConnection conn, final int fetchSize, final XGStatement stmt,
			final ClientWireProtocol.ResultSet re) throws Exception {
		this.conn = conn;
		this.fetchSize = fetchSize;
		this.stmt = stmt;
		requestMetaData();
		mergeData(re);
	}

	@Override
	public boolean absolute(final int row) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void addWarnings(final ArrayList<SQLWarning> ws) {
		warnings.addAll(ws);
	}

	public void addWarnings(final SQLWarning w) {
		warnings.add(w);
	}

	@Override
	public void afterLast() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void beforeFirst() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearWarnings() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		warnings.clear();
	}

	@Override
	public void close() throws SQLException {
		if (closed) {
			return;
		}

		try {
			closed = true;
			sendCloseRS();
		} catch (final Exception e) {
			throw SQLStates.newGenericException(e);
		}
	}

	@Override
	public void deleteRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int findColumn(final String columnLabel) throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Integer pos = cols2Pos.get(columnLabel);
		if (pos == null) {
			throw SQLStates.COLUMN_NOT_FOUND.clone();
		}

		return pos + 1;
	}

	@Override
	public boolean first() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray(final int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray(final String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getAsciiStream(final int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getAsciiStream(final String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
		wasNull = false;

		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			throw SQLStates.CURSOR_NOT_ON_ROW.clone();
		}

		final ArrayList<Object> alo = (ArrayList<Object>) row;

		if (columnIndex < 1 || columnIndex > alo.size()) {
			throw SQLStates.COLUMN_NOT_FOUND.clone();
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null) {
			wasNull = true;
			return null;
		}

		if (!(col instanceof Byte || col instanceof Integer || col instanceof Short || col instanceof Long
				|| col instanceof Float || col instanceof Double || col instanceof BigDecimal)) {
			throw SQLStates.INVALID_DATA_TYPE_CONVERSION.clone();
		}

		final Number num = (Number) col;
		final BigDecimal retval = new BigDecimal(num.doubleValue());
		return retval;
	}

	@Override
	public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
		final BigDecimal retval = getBigDecimal(columnIndex);
		retval.setScale(scale, RoundingMode.HALF_UP);
		return retval;
	}

	@Override
	public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
		return getBigDecimal(cols2Pos.get(columnLabel) + 1);
	}

	@Override
	public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
		final BigDecimal retval = getBigDecimal(columnLabel);
		retval.setScale(scale, RoundingMode.HALF_UP);
		return retval;
	}

	@Override
	public InputStream getBinaryStream(final int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getBinaryStream(final String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(final int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(final String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean getBoolean(final int columnIndex) throws SQLException {
		wasNull = false;

		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			throw SQLStates.CURSOR_NOT_ON_ROW.clone();
		}

		final ArrayList<Object> alo = (ArrayList<Object>) row;

		if (columnIndex < 1 || columnIndex > alo.size()) {
			throw SQLStates.COLUMN_NOT_FOUND.clone();
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null) {
			wasNull = true;
			return false;
		}

		if (!(col instanceof Boolean)) {
			throw SQLStates.INVALID_DATA_TYPE_CONVERSION.clone();
		}

		return (Boolean) col;
	}

	@Override
	public boolean getBoolean(final String columnLabel) throws SQLException {
		return getBoolean(cols2Pos.get(columnLabel) + 1);
	}

	@Override
	public byte getByte(final int columnIndex) throws SQLException {
		wasNull = false;

		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			throw SQLStates.CURSOR_NOT_ON_ROW.clone();
		}

		final ArrayList<Object> alo = (ArrayList<Object>) row;

		if (columnIndex < 1 || columnIndex > alo.size()) {
			throw SQLStates.COLUMN_NOT_FOUND.clone();
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null) {
			wasNull = true;
			return 0;
		}

		if (!(col instanceof Byte) && !(col instanceof Short) && !(col instanceof Integer) && !(col instanceof Long)
				&& !(col instanceof Float) && !(col instanceof Double) && !(col instanceof BigDecimal)) {
			throw SQLStates.INVALID_DATA_TYPE_CONVERSION.clone();
		}

		final Number num = (Number) col;
		return num.byteValue();
	}

	@Override
	public byte getByte(final String columnLabel) throws SQLException {
		return getByte(cols2Pos.get(columnLabel) + 1);
	}

	@Override
	public byte[] getBytes(final int columnIndex) throws SQLException {
		wasNull = false;

		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			throw SQLStates.CURSOR_NOT_ON_ROW.clone();
		}

		final ArrayList<Object> alo = (ArrayList<Object>) row;

		if (columnIndex < 1 || columnIndex > alo.size()) {
			throw SQLStates.COLUMN_NOT_FOUND.clone();
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null) {
			wasNull = true;
			return null;
		}

		if (!(col instanceof byte[])) {
			throw SQLStates.INVALID_DATA_TYPE_CONVERSION.clone();
		}

		return (byte[]) col;
	}

	@Override
	public byte[] getBytes(final String columnLabel) throws SQLException {
		return getBytes(cols2Pos.get(columnLabel) + 1);
	}

	@Override
	public Reader getCharacterStream(final int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getCharacterStream(final String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(final int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(final String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getConcurrency() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public String getCursorName() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(final int columnIndex) throws SQLException {
		wasNull = false;

		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			throw SQLStates.CURSOR_NOT_ON_ROW.clone();
		}

		final ArrayList<Object> alo = (ArrayList<Object>) row;

		if (columnIndex < 1 || columnIndex > alo.size()) {
			throw SQLStates.COLUMN_NOT_FOUND.clone();
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null) {
			wasNull = true;
			return null;
		}

		if (!(col instanceof Date)) {
			throw SQLStates.INVALID_DATA_TYPE_CONVERSION.clone();
		}

		return (Date) col;
	}

	@Override
	public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(final String columnLabel) throws SQLException {
		return getDate(cols2Pos.get(columnLabel) + 1);
	}

	@Override
	public Date getDate(final String columnLabel, final Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public double getDouble(final int columnIndex) throws SQLException {
		wasNull = false;

		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			throw SQLStates.CURSOR_NOT_ON_ROW.clone();
		}

		final ArrayList<Object> alo = (ArrayList<Object>) row;

		if (columnIndex < 1 || columnIndex > alo.size()) {
			throw SQLStates.COLUMN_NOT_FOUND.clone();
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null) {
			wasNull = true;
			return 0;
		}
		if (!(col instanceof Byte || col instanceof Integer || col instanceof Short || col instanceof Long
				|| col instanceof Float || col instanceof Double || col instanceof BigDecimal)) {
			throw SQLStates.INVALID_DATA_TYPE_CONVERSION.clone();
		}

		final Number num = (Number) col;
		return num.doubleValue();
	}

	@Override
	public double getDouble(final String columnLabel) throws SQLException {
		return getDouble(cols2Pos.get(columnLabel) + 1);
	}

	public ArrayList<Object> getEntireRow() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			throw SQLStates.CURSOR_NOT_ON_ROW.clone();
		}

		return (ArrayList<Object>) row;
	}

	@Override
	public int getFetchDirection() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public int getFetchSize() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		return fetchSize;
	}

	@Override
	public float getFloat(final int columnIndex) throws SQLException {
		wasNull = false;

		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			throw SQLStates.CURSOR_NOT_ON_ROW.clone();
		}

		final ArrayList<Object> alo = (ArrayList<Object>) row;

		if (columnIndex < 1 || columnIndex > alo.size()) {
			throw SQLStates.COLUMN_NOT_FOUND.clone();
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null) {
			wasNull = true;
			return 0;
		}

		if (!(col instanceof Byte || col instanceof Integer || col instanceof Short || col instanceof Long
				|| col instanceof Float || col instanceof Double || col instanceof BigDecimal)) {
			throw SQLStates.INVALID_DATA_TYPE_CONVERSION.clone();
		}

		final Number num = (Number) col;
		return num.floatValue();
	}

	@Override
	public float getFloat(final String columnLabel) throws SQLException {
		return getFloat(cols2Pos.get(columnLabel) + 1);
	}

	@Override
	public int getHoldability() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public int getInt(final int columnIndex) throws SQLException {
		wasNull = false;

		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			throw SQLStates.CURSOR_NOT_ON_ROW.clone();
		}

		final ArrayList<Object> alo = (ArrayList<Object>) row;

		if (columnIndex < 1 || columnIndex > alo.size()) {
			throw SQLStates.COLUMN_NOT_FOUND.clone();
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null) {
			wasNull = true;
			return 0;
		}
		if (!(col instanceof Byte || col instanceof Integer || col instanceof Short || col instanceof Long
				|| col instanceof Float || col instanceof Double || col instanceof BigDecimal)) {
			throw SQLStates.INVALID_DATA_TYPE_CONVERSION.clone();
		}

		final Number num = (Number) col;
		return num.intValue();
	}

	@Override
	public int getInt(final String columnLabel) throws SQLException {
		return getInt(cols2Pos.get(columnLabel) + 1);
	}

	private int getLength() throws Exception {
		final byte[] inMsg = new byte[4];

		int count = 0;
		while (count < 4) {
			try {
				final int temp = conn.in.read(inMsg, count, 4 - count);
				if (temp == -1) {
					throw SQLStates.UNEXPECTED_EOF.clone();
				}

				count += temp;
			} catch (final Exception e) {
				throw SQLStates.NETWORK_COMMS_ERROR.clone();
			}
		}

		return bytesToInt(inMsg);
	}

	@Override
	public long getLong(final int columnIndex) throws SQLException {
		wasNull = false;

		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			throw SQLStates.CURSOR_NOT_ON_ROW.clone();
		}

		final ArrayList<Object> alo = (ArrayList<Object>) row;

		if (columnIndex < 1 || columnIndex > alo.size()) {
			throw SQLStates.COLUMN_NOT_FOUND.clone();
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null) {
			wasNull = true;
			return 0;
		}

		if (!(col instanceof Byte || col instanceof Integer || col instanceof Short || col instanceof Long
				|| col instanceof Float || col instanceof Double || col instanceof BigDecimal)) {
			throw SQLStates.INVALID_DATA_TYPE_CONVERSION.clone();
		}

		final Number num = (Number) col;
		return num.longValue();
	}

	@Override
	public long getLong(final String columnLabel) throws SQLException {
		return getLong(cols2Pos.get(columnLabel) + 1);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		try {
			return new XGResultSetMetaData(cols2Pos, pos2Cols, cols2Types);
		} catch (final Exception e) {
			if (e instanceof SQLException) {
				throw (SQLException) e;
			}

			throw SQLStates.newGenericException(e);
		}
	}

	/*
	 * Returns true if it actually got data, false if it just received a zero size
	 * (ping) block of data
	 */
	private boolean getMoreData() throws SQLException {
		if(immutable)
		{
			//no data to get as the resultset was prepopulate at construction time.
			return false;
		}

		try {
			// send FetchData request with fetchSize parameter
			final ClientWireProtocol.FetchData.Builder builder = ClientWireProtocol.FetchData.newBuilder();
			builder.setFetchSize(fetchSize);
			final FetchData msg = builder.build();
			final ClientWireProtocol.Request.Builder b2 = ClientWireProtocol.Request.newBuilder();
			b2.setType(ClientWireProtocol.Request.RequestType.FETCH_DATA);
			b2.setFetchData(msg);
			final Request wrapper = b2.build();
			conn.out.write(intToBytes(wrapper.getSerializedSize()));
			wrapper.writeTo(conn.out);
			conn.out.flush();

			// get confirmation and data (fetchSize rows or zero size result set or
			// terminated early with a DataEndMarker)
			final ClientWireProtocol.FetchDataResponse.Builder fdr = ClientWireProtocol.FetchDataResponse.newBuilder();
			final int length = getLength();
			final byte[] data = new byte[length];
			readBytes(data);
			fdr.mergeFrom(data);
			final ConfirmationResponse response = fdr.getResponse();
			final ResponseType rType = response.getType();
			processResponseType(rType, response);
			return mergeData(fdr.getResultSet());
		} catch (final Exception e) {
			if (e instanceof SQLException) {
				throw (SQLException) e;
			}

			throw SQLStates.newGenericException(e);
		}
	}

	@Override
	public Reader getNCharacterStream(final int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getNCharacterStream(final String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob(final int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob(final String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString(final int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString(final String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject(final int columnIndex) throws SQLException {
		wasNull = false;

		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			throw SQLStates.CURSOR_NOT_ON_ROW.clone();
		}

		final ArrayList<Object> alo = (ArrayList<Object>) row;

		if (columnIndex < 1 || columnIndex > alo.size()) {
			throw SQLStates.COLUMN_NOT_FOUND.clone();
		}

		final Object col = alo.get(columnIndex - 1);
		if (col == null) {
			wasNull = true;
		}

		return col;
	}

	@Override
	public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject(final int columnIndex, final Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject(final String columnLabel) throws SQLException {
		return getObject(cols2Pos.get(columnLabel) + 1);
	}

	@Override
	public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject(final String columnLabel, final Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(final int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(final String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getRow() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		if (position == -1) {
			return 0;
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			return 0;
		}

		return (int) (position + 1);
	}

	@Override
	public RowId getRowId(final int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId(final String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public short getShort(final int columnIndex) throws SQLException {
		wasNull = false;

		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			throw SQLStates.CURSOR_NOT_ON_ROW.clone();
		}

		final ArrayList<Object> alo = (ArrayList<Object>) row;

		if (columnIndex < 1 || columnIndex > alo.size()) {
			throw SQLStates.COLUMN_NOT_FOUND.clone();
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null) {
			wasNull = true;
			return 0;
		}

		if (!(col instanceof Byte) && !(col instanceof Short) && !(col instanceof Integer) && !(col instanceof Long)
				&& !(col instanceof Float) && !(col instanceof Double) && !(col instanceof BigDecimal)) {
			throw SQLStates.INVALID_DATA_TYPE_CONVERSION.clone();
		}

		final Number num = (Number) col;
		return num.shortValue();
	}

	@Override
	public short getShort(final String columnLabel) throws SQLException {
		return getShort(cols2Pos.get(columnLabel) + 1);
	}

	@Override
	public SQLXML getSQLXML(final int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML(final String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	private void getStandardResponse() throws Exception {
		final int length = getLength();
		final byte[] data = new byte[length];
		readBytes(data);
		final ConfirmationResponse.Builder rBuild = ConfirmationResponse.newBuilder();
		rBuild.mergeFrom(data);
		final ResponseType rType = rBuild.getType();
		processResponseType(rType, rBuild.build());
	}

	@Override
	public Statement getStatement() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		return stmt;
	}

	@Override
	public String getString(final int columnIndex) throws SQLException {
		wasNull = false;

		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			throw SQLStates.CURSOR_NOT_ON_ROW.clone();
		}

		final ArrayList<Object> alo = (ArrayList<Object>) row;

		if (columnIndex < 1 || columnIndex > alo.size()) {
			throw SQLStates.COLUMN_NOT_FOUND.clone();
		}

		Object col = alo.get(columnIndex - 1);

		if (col == null) {
			wasNull = true;
			return null;
		}

		if (!(col instanceof String)) {
			col = col.toString();
		}

		return (String) col;
	}

	@Override
	public String getString(final String columnLabel) throws SQLException {
		return getString(cols2Pos.get(columnLabel) + 1);
	}

	@Override
	public Time getTime(final int columnIndex) throws SQLException {
		wasNull = false;

		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			throw SQLStates.CURSOR_NOT_ON_ROW.clone();
		}

		final ArrayList<Object> alo = (ArrayList<Object>) row;

		if (columnIndex < 1 || columnIndex > alo.size()) {
			throw SQLStates.COLUMN_NOT_FOUND.clone();
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null) {
			wasNull = true;
			return null;
		}

		if (!(col instanceof Time)) {
			throw SQLStates.INVALID_DATA_TYPE_CONVERSION.clone();
		}

		return (Time) col;
	}

	@Override
	public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(final String columnLabel) throws SQLException {
		return getTime(cols2Pos.get(columnLabel) + 1);
	}

	@Override
	public Time getTime(final String columnLabel, final Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(final int columnIndex) throws SQLException {
		wasNull = false;

		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			throw SQLStates.CURSOR_NOT_ON_ROW.clone();
		}

		final ArrayList<Object> alo = (ArrayList<Object>) row;

		if (columnIndex < 1 || columnIndex > alo.size()) {
			throw SQLStates.COLUMN_NOT_FOUND.clone();
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null) {
			wasNull = true;
			return null;
		}

		if (!(col instanceof Date)) {
			throw SQLStates.INVALID_DATA_TYPE_CONVERSION.clone();
		}

		return new Timestamp(((Date) col).getTime());
	}

	@Override
	public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(final String columnLabel) throws SQLException {
		return getTimestamp(cols2Pos.get(columnLabel) + 1);
	}

	@Override
	public Timestamp getTimestamp(final String columnLabel, final Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getType() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public InputStream getUnicodeStream(final int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getUnicodeStream(final String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL(final int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL(final String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		if (warnings.size() == 0) {
			return null;
		}

		final SQLWarning retval = warnings.get(0);
		SQLWarning current = retval;
		int i = 1;
		while (i < warnings.size()) {
			current.setNextWarning(warnings.get(i));
			current = warnings.get(i);
			i++;
		}

		return retval;
	}

	@Override
	public void insertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		if (position == -1 && rs.size() == 0) {
			while (!getMoreData()) {
			}
		}

		final Object row = rs.get(rs.size() - 1);
		if (!(row instanceof DataEndMarker)) {
			return false;
		}

		if (position < firstRowIs + rs.size() - 1) {
			return false;
		}

		if (position > 0) {
			return true;
		}

		return false;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		if (position != -1) {
			return false;
		}

		if (rs.size() == 0) {
			while (!getMoreData()) {
			}
		}

		final Object row = rs.get(0);
		if (row instanceof DataEndMarker) {
			return false;
		}

		return true;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}

	@Override
	public boolean isFirst() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		if (position != 0) {
			return false;
		}

		if (rs.size() == 0) {
			while (!getMoreData()) {
			}
		}

		final Object row = rs.get(0);
		if (row instanceof DataEndMarker) {
			return false;
		}

		return true;
	}

	@Override
	public boolean isLast() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isWrapperFor(final Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public boolean last() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	private boolean isBufferDem(ByteBuffer bb) {
		return bb.limit() > 8 && bb.get(8) == 0;
	}

	private int bcdLength(int precision) {
		// translated from C++
		int bytesNeeded = (precision + 1) / 2;
		if ((precision % 2) == 0) {
			bytesNeeded += 1;
		}
		return bytesNeeded;
	}

	private BigDecimal getDecimalFromBuffer(final ByteBuffer bb, int offset) {
		// translated from C++

		// read off parameters
		int precision = bb.get(offset);
		offset += 1;
		int scale = bb.get(offset);
		offset += 1;

		// read raw data
		int bytesNeeded = bcdLength(precision);
		byte[] rawPackedBcdData = new byte[bytesNeeded];
		bb.position(offset);
		bb.get(rawPackedBcdData);

		// translate BCD -> character array of numerals
		// leave room for the sign character, but don't bother dealing with scale and
		// decimal point here
		char[] formedDecimalString = new char[precision + 1];

		// sign character
		boolean isPositive = ((rawPackedBcdData[bytesNeeded - 1] & 0x0f) == 0x0c);
		formedDecimalString[0] = isPositive ? '+' : '-';

		// set up starting indices for reading digits out
		int theByte = 0;
		boolean highOrder = (precision % 2) == 1; // first high-order nibble might be filler

		// read digits from nibbles
		for (int i = 0; i < precision; i++) {
			char digit = '0';
			int byteVal = (rawPackedBcdData[theByte] & 0xFF);
			if (highOrder) {
				digit += (byteVal >> 4);
			} else {
				digit += (byteVal & 0x0f);
			}
			formedDecimalString[1 + i] = digit;

			// increment reading indices
			if (highOrder) {
				highOrder = false;
			} else {
				theByte++;
				highOrder = true;
			}
		}

		// set scale now, right at the end
		return ((new BigDecimal(formedDecimalString)).movePointLeft(scale));
	}

	/*
	 * Returns true if we actually received data, false if there was no data to
	 * merge
	 */
	private boolean mergeData(final ClientWireProtocol.ResultSet re) throws SQLException {
		final List<ByteString> buffers = re.getBlobsList();
		this.rs.clear();
		for (final ByteString buffer : buffers) {
			ByteBuffer bb = buffer.asReadOnlyByteBuffer();
			if (isBufferDem(bb)) {
				rs.add(new DataEndMarker());
			} else {
				int numRows = bb.getInt(0);
				int offset = 4;
				for (int i = 0; i < numRows; i++) {
					// Process this row
					final ArrayList<Object> alo = new ArrayList<>();
					int rowLength = bb.getInt(offset);
					int end = offset + rowLength;
					offset += 4;

					while (offset < end) {
						// Get type tag
						byte type = bb.get(offset);
						offset++;
						if (type == 1) // INT
						{
							alo.add(bb.getInt(offset));
							offset += 4;
						} else if (type == 2) // LONG
						{
							alo.add(bb.getLong(offset));
							offset += 8;
						} else if (type == 3) // FLOAT
						{
							alo.add(Float.intBitsToFloat(bb.getInt(offset)));
							offset += 4;
						} else if (type == 4) // DOUBLE
						{
							alo.add(Double.longBitsToDouble(bb.getLong(offset)));
							offset += 8;
						} else if (type == 5) // STRING
						{
							int stringLength = bb.getInt(offset);
							offset += 4;
							byte[] dst = new byte[stringLength];
							bb.position(offset);
							bb.get(dst);
							alo.add(new String(dst, Charsets.UTF_8));
							offset += stringLength;
						} else if (type == 6) // Timestamp
						{
							alo.add(new Date(bb.getLong(offset)));
							offset += 8;
						} else if (type == 7) // Null
						{
							alo.add(null);
						} else if (type == 8) // BOOL
						{
							alo.add((bb.get(offset) != 0));
							offset++;
						} else if (type == 9) // BINARY
						{
							int stringLength = bb.getInt(offset);
							offset += 4;
							byte[] dst = new byte[stringLength];
							bb.position(offset);
							bb.get(dst);
							alo.add(dst);
							offset += stringLength;
						} else if (type == 10) // BYTE
						{
							alo.add(bb.get(offset));
							offset++;
						} else if (type == 11) // SHORT
						{
							alo.add(bb.getShort(offset));
							offset += 2;
						} else if (type == 12) // TIME
						{
							alo.add(new Time(bb.getLong(offset)));
							offset += 8;
						} else if (type == 13) // DECIMAL
						{
							int precision = bb.get(offset);
							alo.add(getDecimalFromBuffer(bb, offset));
							offset += (2 + bcdLength(precision));
						} else {
							throw SQLStates.INVALID_COLUMN_TYPE.clone();
						}
					}

					rs.add(alo);
				}
			}
		}

		return rs.size() > 0;
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean next() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		position++;

		if (firstRowIs - 1 + rs.size() < position) {
			if (rs.size() > 0) {
				final Object row = rs.get(rs.size() - 1);
				if (row instanceof DataEndMarker) {
					return false;
				}
			}

			// call to get more data
			while (!getMoreData()) {
			}
			firstRowIs = position;
		}

		final Object row = rs.get((int) (position - firstRowIs));
		if (row instanceof DataEndMarker) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public boolean previous() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	private void processResponseType(final ResponseType rType, final ConfirmationResponse response)
			throws SQLException {
		if (rType.equals(ResponseType.INVALID)) {
			throw SQLStates.INVALID_RESPONSE_TYPE.clone();
		} else if (rType.equals(ResponseType.RESPONSE_ERROR)) {
			final String reason = response.getReason();
			final String sqlState = response.getSqlState();
			final int code = response.getVendorCode();
			throw new SQLException(reason, sqlState, code);
		} else if (rType.equals(ResponseType.RESPONSE_WARN)) {
			final String reason = response.getReason();
			final String sqlState = response.getSqlState();
			final int code = response.getVendorCode();
			warnings.add(new SQLWarning(reason, sqlState, code));
		}
	}

	private void readBytes(final byte[] bytes) throws Exception {
		int count = 0;
		final int size = bytes.length;
		while (count < size) {
			final int temp = conn.in.read(bytes, count, bytes.length - count);
			if (temp == -1) {
				throw SQLStates.UNEXPECTED_EOF.clone();
			} else {
				count += temp;
			}
		}

		return;
	}

	@Override
	public void refreshRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean relative(final int rows) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	private void requestMetaData() throws Exception {
		// send fetch metadata request
		final ClientWireProtocol.FetchMetadata.Builder builder = ClientWireProtocol.FetchMetadata.newBuilder();
		final FetchMetadata msg = builder.build();
		final ClientWireProtocol.Request.Builder b2 = ClientWireProtocol.Request.newBuilder();
		b2.setType(ClientWireProtocol.Request.RequestType.FETCH_METADATA);
		b2.setFetchMetadata(msg);
		final Request wrapper = b2.build();
		conn.out.write(intToBytes(wrapper.getSerializedSize()));
		wrapper.writeTo(conn.out);
		conn.out.flush();

		// receive response
		final ClientWireProtocol.FetchMetadataResponse.Builder fmdr = ClientWireProtocol.FetchMetadataResponse
				.newBuilder();
		final int length = getLength();
		final byte[] data = new byte[length];
		readBytes(data);
		fmdr.mergeFrom(data);
		final ConfirmationResponse response = fmdr.getResponse();
		final ResponseType rType = response.getType();
		processResponseType(rType, response);
		cols2Pos = fmdr.getCols2PosMap();
		cols2Types = fmdr.getCols2TypesMap();
		pos2Cols = new TreeMap<>();
		for (final Map.Entry<String, Integer> entry : cols2Pos.entrySet()) {
			pos2Cols.put(entry.getValue(), entry.getKey());
		}
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		return false;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		return false;
	}

	private void sendCloseRS() throws Exception {
		// send CloseResultSet request
		final ClientWireProtocol.CloseResultSet.Builder builder = ClientWireProtocol.CloseResultSet.newBuilder();
		final CloseResultSet msg = builder.build();
		final ClientWireProtocol.Request.Builder b2 = ClientWireProtocol.Request.newBuilder();
		b2.setType(ClientWireProtocol.Request.RequestType.CLOSE_RESULT_SET);
		b2.setCloseResultSet(msg);
		final Request wrapper = b2.build();

		try {
			conn.out.write(intToBytes(wrapper.getSerializedSize()));
			wrapper.writeTo(conn.out);
			conn.out.flush();
			getStandardResponse();
		} catch (final IOException e) {
			// Doesn't matter...
		}
	}

	@Override
	public void setFetchDirection(final int direction) throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		if (direction != ResultSet.FETCH_FORWARD) {
			throw new SQLFeatureNotSupportedException();
		}
	}

	@Override
	public void setFetchSize(final int rows) throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		if (rows <= 0) {
			throw SQLStates.INVALID_ARGUMENT.clone();
		}

		fetchSize = rows;
	}

	@Override
	public <T> T unwrap(final Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateArray(final int columnIndex, final Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateArray(final String columnLabel, final Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(final int columnIndex, final InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(final int columnIndex, final InputStream x, final int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(final int columnIndex, final InputStream x, final long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(final String columnLabel, final InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(final String columnLabel, final InputStream x, final int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(final String columnLabel, final InputStream x, final long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBigDecimal(final int columnIndex, final BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBigDecimal(final String columnLabel, final BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(final int columnIndex, final InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(final int columnIndex, final InputStream x, final int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(final int columnIndex, final InputStream x, final long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(final String columnLabel, final InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(final String columnLabel, final InputStream x, final int length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(final String columnLabel, final InputStream x, final long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(final int columnIndex, final Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(final int columnIndex, final InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(final int columnIndex, final InputStream inputStream, final long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(final String columnLabel, final Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(final String columnLabel, final InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(final String columnLabel, final InputStream inputStream, final long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBoolean(final int columnIndex, final boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBoolean(final String columnLabel, final boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateByte(final int columnIndex, final byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateByte(final String columnLabel, final byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBytes(final int columnIndex, final byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBytes(final String columnLabel, final byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(final int columnIndex, final Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(final int columnIndex, final Reader x, final int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(final String columnLabel, final Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(final String columnLabel, final Reader reader, final int length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(final String columnLabel, final Reader reader, final long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(final int columnIndex, final Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(final int columnIndex, final Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(final int columnIndex, final Reader reader, final long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(final String columnLabel, final Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(final String columnLabel, final Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(final String columnLabel, final Reader reader, final long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDate(final int columnIndex, final Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDate(final String columnLabel, final Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDouble(final int columnIndex, final double x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDouble(final String columnLabel, final double x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateFloat(final int columnIndex, final float x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateFloat(final String columnLabel, final float x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateInt(final int columnIndex, final int x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateInt(final String columnLabel, final int x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateLong(final int columnIndex, final long x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateLong(final String columnLabel, final long x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(final int columnIndex, final Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(final String columnLabel, final Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(final String columnLabel, final Reader reader, final long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(final int columnIndex, final NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(final int columnIndex, final Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(final int columnIndex, final Reader reader, final long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(final String columnLabel, final NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(final String columnLabel, final Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(final String columnLabel, final Reader reader, final long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNString(final int columnIndex, final String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNString(final String columnLabel, final String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNull(final int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNull(final String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(final int columnIndex, final Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(final int columnIndex, final Object x, final int scaleOrLength) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(final String columnLabel, final Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(final String columnLabel, final Object x, final int scaleOrLength) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRef(final int columnIndex, final Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	@Override
	public void updateRef(final String columnLabel, final Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRowId(final int columnIndex, final RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRowId(final String columnLabel, final RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateShort(final int columnIndex, final short x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateShort(final String columnLabel, final short x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateSQLXML(final int columnIndex, final SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateSQLXML(final String columnLabel, final SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateString(final int columnIndex, final String x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateString(final String columnLabel, final String x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTime(final int columnIndex, final Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTime(final String columnLabel, final Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTimestamp(final int columnIndex, final Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTimestamp(final String columnLabel, final Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean wasNull() throws SQLException {
		if (closed) {
			throw SQLStates.CALL_ON_CLOSED_OBJECT.clone();
		}

		return wasNull;
	}
}
