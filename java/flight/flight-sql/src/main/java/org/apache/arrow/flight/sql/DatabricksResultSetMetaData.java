package org.apache.arrow.flight.sql;

import java.sql.*;
import java.util.*;

public class DatabricksResultSetMetaData implements ResultSetMetaData {
    private final List<String> columnNames;
    private final List<String> columnTypeNames;
    private final List<Integer> columnTypes;

    public DatabricksResultSetMetaData(List<String> columnNames,
                                       List<String> columnTypeNames,
                                       List<Integer> columnTypes) {
        this.columnNames = columnNames;
        this.columnTypeNames = columnTypeNames;
        this.columnTypes = columnTypes;
    }

    @Override
    public int getColumnCount() {
        return columnNames.size();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return false;
    }

    @Override
    public boolean isSearchable(int column) {
        return true;
    }

    @Override
    public boolean isCurrency(int column) {
        return false;
    }

    @Override
    public int isNullable(int column) {
        return 0;
    }

    @Override
    public boolean isSigned(int column) {
        column = column - 1;
        return (columnTypes.get(column) == Types.INTEGER
                || columnTypes.get(column) == Types.DECIMAL
                || columnTypes.get(column) == Types.BIGINT
                || columnTypes.get(column) == Types.DOUBLE);
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return 25;
    }

    @Override
    public String getColumnLabel(int column) {
        if (columnNames != null) {
            return columnNames.get(column - 1);
        } else {
            return "Column - " + column;
        }
    }

    @Override
    public String getColumnName(int column) {
        return columnNames.get(column - 1);
    }

    @Override
    public String getSchemaName(int column) {
        return null;
    }

    @Override
    public int getPrecision(int column) {
        // For string.
        return 255;
    }

    @Override
    public int getScale(int column) {
        // I do not know about this.
        return 0;
    }

    @Override
    public String getTableName(int column) {
        return null;
    }

    @Override
    public String getCatalogName(int column) {
        return null;
    }

    @Override
    public int getColumnType(int column) {
        return columnTypes.get(column - 1);
    }

    @Override
    public String getColumnTypeName(int column) {
        return columnTypeNames.get(column - 1);
    }

    @Override
    public boolean isReadOnly(int column) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isWritable(int column) {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    @Override
    public String getColumnClassName(int column) {
        return String.class.getCanonicalName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
