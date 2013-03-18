package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.PSQLExceptions.COLUMN_INDEX_OUT_OF_BOUNDS;
import static com.impossibl.postgres.jdbc.PSQLTypeMetaData.getSQLType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.system.Settings;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.Type;

class PSQLResultSetMetaData implements ResultSetMetaData {

	PSQLConnection connection;
	List<ResultField> resultFields;
	
	PSQLResultSetMetaData(PSQLConnection connection, List<ResultField> resultFields) {
		this.connection = connection;
		this.resultFields = resultFields;
	}

	/**
	 * Returns the ResultField associated with the requested column
	 *  
	 * @param columnIndex Requested column index
	 * @return ResultField of column
	 * @throws SQLException If columnIndex is out of bounds
	 */
	ResultField get(int columnIndex) throws SQLException {
		
		if(columnIndex < 1 || columnIndex > resultFields.size()) {
			throw COLUMN_INDEX_OUT_OF_BOUNDS;
		}
		
		return resultFields.get(columnIndex-1);
	}

	/**
	 * Returns the CompositeType representing the requested column's table.
	 *  
	 * @param columnIndex Requested column index
	 * @return CompositeType of columns table
	 * @throws SQLException If columnIndex is out of bounds
	 */
	CompositeType getRelType(int columnIndex) throws SQLException {
		
		ResultField field = get(columnIndex);
		if(field.relationId == 0)
			return null;
					
		return connection.getRegistry().loadRelationType(field.relationId);
	}

	/**
	 * Returns the CompositeType.Attribute representing the requested column
	 * 
	 * @param columnIndex Requested column index
	 * @return CompositeType.Attribute of the requested column
	 * @throws SQLException If columnIndex is out of bounds
	 */
	CompositeType.Attribute getRelAttr(int columnIndex) throws SQLException {

		ResultField field = get(columnIndex);
		
		CompositeType relType = connection.getRegistry().loadRelationType(field.relationId);
		if(relType == null)
			return null;
		
		return relType.getAttribute(field.relationAttributeNumber);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return iface.cast(this);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isAssignableFrom(getClass());
	}

	@Override
	public int getColumnCount() throws SQLException {
		return resultFields.size();
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		
		CompositeType.Attribute relAttr = getRelAttr(column);
		if(relAttr == null) {
			return false;
		}
		
		return relAttr.autoIncrement;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		
		return PSQLTypeMetaData.isCaseSensitive(get(column).type);
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		//TODO is there any case that a column is not searchable
		return true;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {

		return PSQLTypeMetaData.isCurrency(get(column).type);
	}

	@Override
	public int isNullable(int column) throws SQLException {
		
		ResultField field = get(column);
		CompositeType relType = connection.getRegistry().loadRelationType(field.relationId);
		
		return PSQLTypeMetaData.isNullable(field.type, relType, field.relationAttributeNumber);
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		
		return PSQLTypeMetaData.isSigned(get(column).type);
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {

		return get(column).name;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		
		return connection.getSetting(Settings.DATABASE).toString();
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		
		Type relType = getRelType(column);
		if(relType == null)
			return null;
		
		return relType.getNamespace();
	}

	@Override
	public String getTableName(int column) throws SQLException {
		
		//Note: there seems to be some debate about whether this should return
		//query aliases or table names. We are returning table names, if 
		//available, as this is at least more useful than always returning
		//null since we never have the query table aliases
		
		CompositeType relType = getRelType(column);
		if(relType == null)
			return null;
		
		return relType.getName();
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		
		CompositeType.Attribute attr = getRelAttr(column);
		if(attr == null)
			return null;
		
		return attr.name;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		ResultField field = get(column);
		return getSQLType(field.type);
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		
		return get(column).type.getName();
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		
		ResultField field = get(column);
		return field.type.getOutputType(field.format).toString();
	}

	@Override
	public int getPrecision(int column) throws SQLException {

		ResultField field = get(column);
		return PSQLTypeMetaData.getPrecision(field.type, field.typeLength, field.typeModifier);
	}

	@Override
	public int getScale(int column) throws SQLException {

		ResultField field = get(column);
		return PSQLTypeMetaData.getScale(field.type, field.typeLength, field.typeModifier);
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {

		ResultField field = get(column);
		return PSQLTypeMetaData.getDisplaySize(field.type, field.typeLength, field.typeModifier);
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		
		//If it's a computed column we assume it's read only
		return get(column).relationAttributeNumber == 0;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		return !isReadOnly(column);
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		//TODO determine what this is really asking
		return isWritable(column);
	}

}
