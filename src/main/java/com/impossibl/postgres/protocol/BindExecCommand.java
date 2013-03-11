package com.impossibl.postgres.protocol;

import java.util.List;

import com.impossibl.postgres.protocol.v30.BindExecCommandImpl.Status;
import com.impossibl.postgres.types.Type;

public interface BindExecCommand extends QueryCommand {
	
	Status getStatus();

	int getMaxRows();
	public void setMaxRows(int maxRows);
	
	int getMaxFieldLength();
	public void setMaxFieldLength(int maxFieldLength);
	
	String getStatementName();
	String getPortalName();
	
	List<Type> getParameterTypes();
	List<Object> getParameterValues();
	
	List<ResultField> getResultFields();
	<T> List<T> getResults(Class<T> rowType);
	
}
