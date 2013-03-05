package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;


public class QueryProtocol extends AbstractQueryProtocol {	
	
	public enum Status {
		Completed,
		Suspended
	}
	
	private List<Type> parameterTypes;
	private Class<?> rowType;
	private List<ResultField> resultFields;
	private List<Object> results;
	private Status status;
	

	public QueryProtocol(Context context, Class<?> rowType) {
		super(context);
		this.parameterTypes = Collections.emptyList();
		this.rowType = rowType;
		this.resultFields = null;
		this.results = new ArrayList<>();
	}
	
	@Override
	public boolean isRunComplete() {
		return super.isRunComplete() || status != null;
	}

	public List<Type> getParameterTypes() {
		return parameterTypes;
	}
	
	@Override
	protected List<ResultField> getResultFields() {
		return resultFields;
	}

	@Override
	protected Class<?> getRowType() {
		return rowType;
	}

	public List<Object> getResults() {
		return results;
	}

	public Status getStatus() {
		return status;
	}
	
	protected void parameterDescriptions(List<Type> paramTypes) {
		parameterTypes = paramTypes;
	}

	protected void rowDescription(List<ResultField> resultFields) {
		this.resultFields = resultFields;
	}

	protected void rowData(Object rowInstance) throws IOException {
		results.add(rowInstance);
	}
	
	protected void portalSuspended() {
		status = Status.Suspended;
	}
	
	protected void noData() {
		resultFields = Collections.emptyList();
	}
	
	protected void parseComplete() {
	}
	
	protected void bindComplete() {
	}
	
	protected void closeComplete() {
	}
	
	protected void emptyQuery() {
		status = Status.Completed;
	}
	
	@Override
	protected void commandComplete(String commandTag) {
		super.commandComplete(commandTag);
		status = Status.Completed;
	}
	
}
