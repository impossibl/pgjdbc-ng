package com.impossibl.postgres.protocol;

import java.util.List;

import com.impossibl.postgres.types.Type;

public interface BindExecCommand extends QueryCommand {
	
	String getStatementName();
	String getPortalName();
		
	List<Type> getParameterTypes();
	void setParameterTypes(List<Type> parameterTypes);
	
	List<Object> getParameterValues();
	void setParameterValues(List<Object> values);
	
}
