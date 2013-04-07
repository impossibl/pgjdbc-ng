package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.ArrayUtils.getDimensions;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToArray;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

public class PGArray implements Array {

	PGConnection connection;
	ArrayType type;
	Object[] value;

	public PGArray(PGConnection connection, ArrayType type, Object[] value) {
		super();
		this.connection = connection;
		this.type = type;
		this.value = value;
	}

	public Object[] getValue() {
		return value;
	}

	@Override
	public String getBaseTypeName() throws SQLException {
		return type.getElementType().getName();
	}

	@Override
	public int getBaseType() throws SQLException {
		return SQLTypeMetaData.getSQLType(type.getElementType());
	}

	@Override
	public Object getArray() throws SQLException {
		return getArray(connection.getTypeMap());
	}

	@Override
	public Object getArray(Map<String, Class<?>> map) throws SQLException {
		
		Class<?> targetType = SQLTypeUtils.mapType(type, map);

		return coerceToArray(value, type, targetType, map, connection);
	}

	@Override
	public Object getArray(long index, int count) throws SQLException {
		return getArray(index, count, connection.getTypeMap());
	}

	@Override
	public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {

		if(index < 1 || index > value.length || (index + count) > (value.length + 1)) {
			 throw new SQLException("Invalid array slice");
		}
		
		Class<?> targetType = SQLTypeUtils.mapType(type, map);

		return coerceToArray(value, (int)index-1, count, type, targetType, map, connection);
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return getResultSet(connection.getTypeMap());
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
		return getResultSet(1, value.length, map);
	}

	@Override
	public ResultSet getResultSet(long index, int count) throws SQLException {
		return getResultSet(index, count, connection.getTypeMap());
	}

	@Override
	public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {

		if(index < 1 || index > (value.length + 1) || (index + count) > (value.length + 1)) {
			 throw new SQLException("Invalid array slice");
		}

		Registry reg = connection.getRegistry();
		
		Type elementType = getDimensions(value) > 1 ? type : type.getElementType();
		
		ResultField[] fields = {
				new ResultField("INDEX", 0, (short)0, reg.loadType("int4"), (short)0, 0, Format.Binary),
				new ResultField("VALUE", 0, (short)0, elementType, (short)0, 0, Format.Binary)
		};
		
		List<Object[]> results = new ArrayList<Object[]>(value.length);
		for(long c=index,end=index+count; c < end; ++c) {
			results.add(new Object[]{c, value[(int) c-1]});
		}
		
		PGStatement stmt = connection.createStatement();
		stmt.closeOnCompletion();
		return stmt.createResultSet(Arrays.asList(fields), results, map);
	}

	@Override
	public void free() throws SQLException {
		connection = null;
		type = null;
		value = null;
	}

}
