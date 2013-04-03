package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.Exceptions.PARAMETER_INDEX_OUT_OF_BOUNDS;
import static com.impossibl.postgres.jdbc.Exceptions.UNWRAP_ERROR;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.types.Type;

public class PGParameterMetaData implements ParameterMetaData {
	
	
	List<Type> parameterTypes;
	Map<String, Class<?>> typeMap;
	
	
	public PGParameterMetaData(List<Type> parameterTypes, Map<String, Class<?>> typeMap) {
		super();
		this.parameterTypes = parameterTypes;
		this.typeMap = typeMap;
	}

	void checkParamIndex(int paramIndex) throws SQLException {

		if(paramIndex < 1 || paramIndex > parameterTypes.size())
			throw PARAMETER_INDEX_OUT_OF_BOUNDS;
		
	}
	
	Type getType(int paramIndex) throws SQLException {
		checkParamIndex(paramIndex);
		
		return parameterTypes.get(paramIndex-1);
	}
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		if(!iface.isAssignableFrom(getClass())) {
			throw UNWRAP_ERROR;
		}

		return iface.cast(this);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isAssignableFrom(getClass());
	}

	@Override
	public int getParameterCount() throws SQLException {
		return parameterTypes.size();
	}

	@Override
	public int isNullable(int param) throws SQLException {

		Type paramType = getType(param);
		
		if(SQLTypeMetaData.isNullable(paramType) == parameterNoNulls) {
			return parameterNoNulls;
		}
		
		return ParameterMetaData.parameterNullable;
	}

	@Override
	public boolean isSigned(int param) throws SQLException {
		
		Type type = getType(param);
		
		return SQLTypeMetaData.isSigned(type);
	}

	@Override
	public int getPrecision(int param) throws SQLException {
		
		Type paramType = getType(param);
		
		return SQLTypeMetaData.getPrecision(paramType, 0, 0);
	}

	@Override
	public int getScale(int param) throws SQLException {

		Type paramType = getType(param);
		
		return SQLTypeMetaData.getScale(paramType, 0, 0);
	}

	@Override
	public int getParameterType(int param) throws SQLException {
		
		Type paramType = getType(param);
		
		return SQLTypeMetaData.getSQLType(paramType);
	}

	@Override
	public String getParameterTypeName(int param) throws SQLException {

		Type paramType = getType(param);
		
		return paramType.getName();
	}

	@Override
	public String getParameterClassName(int param) throws SQLException {

		Type paramType = getType(param);
		
		return paramType.getJavaType(typeMap).getName();
	}

	@Override
	public int getParameterMode(int param) throws SQLException {

		return ParameterMetaData.parameterModeIn;
	}

}
