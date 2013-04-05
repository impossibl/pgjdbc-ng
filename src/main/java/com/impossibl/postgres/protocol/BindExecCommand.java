package com.impossibl.postgres.protocol;

import java.util.List;

import com.impossibl.postgres.types.Type;

public interface BindExecCommand extends QueryCommand {
	
	String getStatementName();
	String getPortalName();
		
	List<Type> getParameterTypes();
	List<Object> getParameterValues();
	
}
