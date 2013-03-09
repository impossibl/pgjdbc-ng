package com.impossibl.postgres.protocol;

import java.util.List;

import com.impossibl.postgres.protocol.v30.QueryCommandImpl.Status;
import com.impossibl.postgres.types.Type;

public interface QueryCommand extends ExecuteCommand {
	
	Status getStatus();

	int getMaxRows();
	public void setMaxRows(int maxRows);
	
	String getStatementName();
	String getPortalName();
	
	List<Type> getParameterTypes();
	List<Object> getParameterValues();
	
	List<ResultField> getResultFields();
	<T> List<T> getResults(Class<T> rowType);
	
}
