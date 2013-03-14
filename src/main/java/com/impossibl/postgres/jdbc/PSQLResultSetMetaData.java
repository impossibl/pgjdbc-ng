package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.PSQLExceptions.COLUMN_INDEX_OUT_OF_BOUNDS;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.Type;

public class PSQLResultSetMetaData implements ResultSetMetaData {

	PSQLConnection connection;
	List<ResultField> resultFields;
	
	public PSQLResultSetMetaData(PSQLConnection connection, List<ResultField> resultFields) {
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
					
		return (CompositeType) connection.getRegistry().loadType(field.relationId);
	}

	/**
	 * Returns the CompositeType.Attribute representing the requested column
	 * 
	 * @param columnIndex Requested column index
	 * @return CompositeType.Attribute of the requested column
	 * @throws SQLException If columnIndex is out of bounds
	 */
	CompositeType.Attribute getRelAttr(int columnIndex) throws SQLException {

		Type relationType = getRelType(columnIndex);
		
		return ((CompositeType)relationType).getAttribute(columnIndex);
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
		ResultField field = get(column);
		return field.type.getInputType(field.format) == String.class;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		//TODO this should be determined, somehow, by relating it to the Moneys codecs
		
		switch(get(column).type.getName()) {
		case "money":
		case "cash":
			return true;
			
		default:
			return false;
		}
		
	}

	@Override
	public int isNullable(int column) throws SQLException {
		
		CompositeType.Attribute attr = getRelAttr(column);
		if(attr == null)
			return columnNullableUnknown;
		
		return attr.nullable ? columnNullable : columnNoNulls;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		
		ResultField field = get(column);
		Class<?> fieldType = field.type.getInputType(field.format);
		
		if(Number.class.isAssignableFrom(fieldType)) {
			return true;
		}
		
		return false;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		//TODO determine good display size for column
		return 0;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return get(column).name;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		//TODO determine catalog names
		return "";
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		//TODO determine schema names
		return "";
	}

	@Override
	public String getTableName(int column) throws SQLException {
		//TODO there seems to be some debate about where this should return query aliases or table names
		
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
		return get(column).type.getSqlType();
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
		//TODO determine precision 
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException {
		//TODO determine scale 
		return 0;
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		
		//If it's a computed column we assume it's read only
		return getRelAttr(column) == null;
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
