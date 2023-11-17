package org.apache.arrow.flight.sql;

import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.databricks.sdk.service.sql.ResultSchema;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class DatabricksResultSet implements ResultSet {
    private final DatabricksResultSetMetaData resultSetMetaData;
    private final List<List<String>> rows;
    private int row = -1;

    public DatabricksResultSet(
            List<String> columnNames,
            List<String> columnTypeNames,
            List<Integer> columnTypes,
            List<List<String>> rows) {
        this.resultSetMetaData = new DatabricksResultSetMetaData(columnNames, columnTypeNames, columnTypes);
        this.rows = rows;
    }

    public List<List<String>> getRows() {
        return this.rows;
    }

    private static int toTypeInt(ColumnInfoTypeName c) {
        if (c.equals(ColumnInfoTypeName.INT)) return Types.INTEGER;
        if (c.equals(ColumnInfoTypeName.BOOLEAN)) return Types.BOOLEAN;
        if (c.equals(ColumnInfoTypeName.STRING)) return Types.VARCHAR;
        if (c.equals(ColumnInfoTypeName.DECIMAL)) return Types.DECIMAL;
        if (c.equals(ColumnInfoTypeName.DATE)) return Types.DATE;
        if (c.equals(ColumnInfoTypeName.DOUBLE)) return Types.DOUBLE;
        if (c.equals(ColumnInfoTypeName.FLOAT)) return Types.FLOAT;
        if (c.equals(ColumnInfoTypeName.LONG)) return Types.BIGINT;
        if (c.equals(ColumnInfoTypeName.TIMESTAMP)) return Types.TIMESTAMP;
        throw new UnsupportedOperationException("Unknown ColumnInfoTypeName " + c);
    }

    public static DatabricksResultSet of(QueryResult queryResult) {
        List<String> columnNames = new ArrayList<>();
        List<String> columnTypeNames = new ArrayList<>();
        List<Integer> columnTypes = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        if (queryResult != null && queryResult.getResultMetadata() != null && queryResult.getResultMetadata().getSchema() != null) {
            ResultSchema schema = queryResult.getResultMetadata().getSchema();
            if (schema.getColumns() != null) {
                for (ColumnInfo column : schema.getColumns()) {
                    columnNames.add(column.getName());
                    columnTypeNames.add(column.getTypeText());
                    columnTypes.add(toTypeInt(column.getTypeName()));
                }
                rows = queryResult.getRows();
            }
        }
        return new DatabricksResultSet(columnNames, columnTypeNames, columnTypes, rows);
    }

    @Override
    public boolean next() {
        row++;
        return row < rows.size();
    }

    @Override
    public void close() {

    }

    @Override
    public boolean wasNull() {
        return false;
    }

    @Override
    public String getString(int columnIndex) {
        return cell(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        String v = cell(columnIndex);
        return v == null ? false : Boolean.parseBoolean(v);
    }

    @Override
    public byte getByte(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public short getShort(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getInt(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long getLong(int columnIndex) {
        String v = cell(columnIndex);
        return v == null ? 0 : Long.parseLong(v);
    }

    @Override
    public float getFloat(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public double getDouble(int columnIndex) {
        String v = cell(columnIndex);
        return v == null ? 0.0 : Double.parseDouble(v);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        return new byte[0];
    }

    @Override
    public Date getDate(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Time getTime(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    private static String ISO_DATE_FORMAT_ZERO_OFFSET = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static String UTC_TIMEZONE_NAME = "UTC";
    private static SimpleDateFormat simpleDateFormat;

    {
        simpleDateFormat = new SimpleDateFormat(ISO_DATE_FORMAT_ZERO_OFFSET);
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone(UTC_TIMEZONE_NAME));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) {
        // e.g '2021-01-01T05:30:27.000Z' could be the return value.
        String cell = cell(columnIndex);
        if (cell == null) return null;
        try {
            java.util.Date d = simpleDateFormat.parse(cell);
            return new Timestamp(d.getTime());
        } catch (ParseException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getString(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean getBoolean(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public byte getByte(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public short getShort(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getInt(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long getLong(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public float getFloat(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public double getDouble(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public byte[] getBytes(String columnLabel) {
        return new byte[0];
    }

    @Override
    public Date getDate(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Time getTime(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {

    }

    @Override
    public String getCursorName() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return this.resultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Object getObject(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int findColumn(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Reader getCharacterStream(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isBeforeFirst() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isAfterLast() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isFirst() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isLast() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void beforeFirst() {

    }

    @Override
    public void afterLast() {

    }

    @Override
    public boolean first() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean last() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getRow() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean absolute(int row) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean relative(int rows) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean previous() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setFetchDirection(int direction) {

    }

    @Override
    public int getFetchDirection() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setFetchSize(int rows) {

    }

    @Override
    public int getFetchSize() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getType() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getConcurrency() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean rowUpdated() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean rowInserted() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean rowDeleted() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void updateNull(int columnIndex) {

    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) {

    }

    @Override
    public void updateByte(int columnIndex, byte x) {

    }

    @Override
    public void updateShort(int columnIndex, short x) {

    }

    @Override
    public void updateInt(int columnIndex, int x) {

    }

    @Override
    public void updateLong(int columnIndex, long x) {

    }

    @Override
    public void updateFloat(int columnIndex, float x) {

    }

    @Override
    public void updateDouble(int columnIndex, double x) {

    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) {

    }

    @Override
    public void updateString(int columnIndex, String x) {

    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) {

    }

    @Override
    public void updateDate(int columnIndex, Date x) {

    }

    @Override
    public void updateTime(int columnIndex, Time x) {

    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) {

    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) {

    }

    @Override
    public void updateObject(int columnIndex, Object x) {

    }

    @Override
    public void updateNull(String columnLabel) {

    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) {

    }

    @Override
    public void updateByte(String columnLabel, byte x) {

    }

    @Override
    public void updateShort(String columnLabel, short x) {

    }

    @Override
    public void updateInt(String columnLabel, int x) {

    }

    @Override
    public void updateLong(String columnLabel, long x) {

    }

    @Override
    public void updateFloat(String columnLabel, float x) {

    }

    @Override
    public void updateDouble(String columnLabel, double x) {

    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) {

    }

    @Override
    public void updateString(String columnLabel, String x) {

    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) {

    }

    @Override
    public void updateDate(String columnLabel, Date x) {

    }

    @Override
    public void updateTime(String columnLabel, Time x) {

    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) {

    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) {

    }

    @Override
    public void updateObject(String columnLabel, Object x) {

    }

    @Override
    public void insertRow() {

    }

    @Override
    public void updateRow() {

    }

    @Override
    public void deleteRow() {

    }

    @Override
    public void refreshRow() {

    }

    @Override
    public void cancelRowUpdates() {

    }

    @Override
    public void moveToInsertRow() {

    }

    @Override
    public void moveToCurrentRow() {

    }

    @Override
    public Statement getStatement() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Ref getRef(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Blob getBlob(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Clob getClob(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Array getArray(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Ref getRef(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Blob getBlob(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Clob getClob(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Array getArray(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public URL getURL(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public URL getURL(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) {

    }

    @Override
    public void updateRef(String columnLabel, Ref x) {

    }

    @Override
    public void updateBlob(int columnIndex, Blob x) {

    }

    @Override
    public void updateBlob(String columnLabel, Blob x) {

    }

    @Override
    public void updateClob(int columnIndex, Clob x) {

    }

    @Override
    public void updateClob(String columnLabel, Clob x) {

    }

    @Override
    public void updateArray(int columnIndex, Array x) {

    }

    @Override
    public void updateArray(String columnLabel, Array x) {

    }

    @Override
    public RowId getRowId(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public RowId getRowId(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) {

    }

    @Override
    public void updateRowId(String columnLabel, RowId x) {

    }

    @Override
    public int getHoldability() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isClosed() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void updateNString(int columnIndex, String nString) {

    }

    @Override
    public void updateNString(String columnLabel, String nString) {

    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) {

    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) {

    }

    @Override
    public NClob getNClob(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public NClob getNClob(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) {

    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) {

    }

    @Override
    public String getNString(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getNString(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) {

    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader) {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader) {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) {

    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        ResultSet.super.updateObject(columnIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)  throws SQLException {
        ResultSet.super.updateObject(columnLabel, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        ResultSet.super.updateObject(columnIndex, x, targetSqlType);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        ResultSet.super.updateObject(columnLabel, x, targetSqlType);
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** Column index starts from 1, not 0. */
    private int columnIndex(int columnIndex) {
        return columnIndex - 1;
    }

    private String cell(int columnIndex) {
        Object cell = rows.get(row).get(columnIndex(columnIndex));
        return cell == null ? null : (String) cell;
    }
}
