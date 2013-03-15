package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.PSQLExceptions.COLUMN_INDEX_OUT_OF_BOUNDS;
import static com.impossibl.postgres.types.Modifiers.LENGTH;
import static com.impossibl.postgres.types.Modifiers.PRECISION;
import static com.impossibl.postgres.types.Modifiers.SCALE;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.system.Settings;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.DomainType;
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
		
		return relType.getAttribute(field.relationAttributeIndex-1);
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
		
		switch(get(column).type.unwrap().getCategory()) {
		case Enumeration:
		case String:
			return true;
			
		default:
			return false;
		}		
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		//TODO is there any case that a column is not searchable
		return true;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		//TODO this should be determined, somehow, by relating it to the Moneys codecs
		//or maybe we should parse a special money type that fits in easily
		
		switch(get(column).type.unwrap().getName()) {
		case "money":
		case "cash":
			return true;
			
		default:
			return false;
		}
		
	}

	@Override
	public int isNullable(int column) throws SQLException {
		
		//Check attributes for nullability
		CompositeType.Attribute attr = getRelAttr(column);
		if(attr != null) {		
			return attr.nullable ? columnNullable : columnNoNulls;
		}
		
		//Check domain types for nullability
		Type type = get(column).type;
		if(type instanceof DomainType) {
			return ((DomainType) type).isNullable() ? columnNullable : columnNoNulls;
		}
		
		//Everything else... we just don't know
		return columnNullableUnknown;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		
		switch(get(column).type.unwrap().getCategory()) {
		case Numeric:
			return true;
			
		default:
			return false;
		}
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		//TODO determine good display size for columns
		return 0;
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

		ResultField field = get(column);
		Type type = field.type.unwrap();
		Class<?> javaType = field.type.getOutputType(field.format);		
		Map<String, Object> mods = type.getModifierParser().parse(field.typeModifier);

		//Lookup prec & length (if the mods have them)
		
		int precMod = 0;
		if(mods.containsKey(PRECISION)) {
			precMod = (int) mods.get(PRECISION);
		}
		
		int lenMod = 0;
		if(mods.containsKey(LENGTH)) {
			lenMod = (int) mods.get(LENGTH);
		}
		else if(field.typeLength != -1) {
			lenMod = field.typeLength;
		}
		
		//Calculate prec
		
		int prec = 0;
		
		switch(type.getCategory()) {
		case Numeric:
			
			if(javaType == BigDecimal.class) {
				if(precMod != 0) {
					prec = precMod;
				}
				else {
					if(isCurrency(column))
						prec = 19;
					else
						prec = 131072;
				}
			}
			else if(javaType == Short.class) {
				prec = 5;
			}
			else if(javaType == Integer.class) {
				prec = 10;
			}
			else if(javaType == Long.class) {
				prec = 19;
			}
			else if(javaType == Float.class) {
				prec = 8;
			}
			else if(javaType == Double.class) {
				prec = 17;
			}
			break;
			
		case DateTime:
			prec = getColumnDisplaySize(column);
			break;
			
		case String:
		case Enumeration:
		case BitString:
			prec = lenMod;
			break;
			
		default:
			prec = lenMod;
		}
		
		return prec;
	}

	@Override
	public int getScale(int column) throws SQLException {

		ResultField field = get(column);
		Map<String, Object> mods = field.type.unwrap().getModifierParser().parse(field.typeModifier);
		
		Object scale = mods.get(SCALE);
		if(scale == null)
			return 0;
		
		return (int) scale;
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		
		//If it's a computed column we assume it's read only
		return get(column).relationAttributeIndex == 0;
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
