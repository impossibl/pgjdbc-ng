package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerce;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.mapGetType;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.CompositeType.Attribute;

public class PGStruct implements Struct {

	PGConnection connection;
	CompositeType type;
	Object[] values;

	public PGStruct(PGConnection connection, CompositeType type, Object[] values) {
		super();
		this.connection = connection;
		this.type = type;
		this.values = values;
	}

	public CompositeType getType() {
		return type;
	}

	@Override
	public String getSQLTypeName() throws SQLException {
		return type.getName();
	}

	@Override
	public Object[] getAttributes() throws SQLException {
		
		return getAttributes(connection.getTypeMap());
	}

	@Override
	public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {

		Object[] newValues = new Object[values.length];
		
		List<Attribute> attrs = type.getAttributes();
		
		for(int c=0; c < attrs.size(); c++) {
			
			Attribute attr = attrs.get(c);
			
			Class<?> targetType = mapGetType(type, map);
			
			newValues[c] = coerce(values[c], attr.type, targetType, map, connection);
		}
		
		return newValues;
	}

}
