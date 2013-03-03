package com.impossibl.postgres.protocol;

import java.util.ArrayList;
import java.util.List;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Tuple;
import com.impossibl.postgres.types.Type;

public class Query extends BaseResponseHandler {

	Class<?> rowType;
	List<Type> parameterTypes;
	Tuple resultsType;
	List<Object> results;
	String completionTag;
	
	public Query(Context context, Class<?> rowType) {
		super(context);
		this.parameterTypes = new ArrayList<Type>();
		this.rowType = rowType;
		this.results = new ArrayList<Object>();
	}
	
	@Override
	public Context getContext() {
		return context;
	}
	
	@Override
	public boolean isComplete() {
		return completionTag != null || error != null;
	}

	@Override
	public void ready(TransactionStatus txStatus) {
	}
	
	@Override
	public List<Type> getParameterTypes() {
		return parameterTypes;
	}

	@Override
	public void setParameterTypes(List<Type> parameterTypes) {
		this.parameterTypes = parameterTypes;
	}

	@Override
	public Tuple getResultsType() {
		return resultsType;
	}

	@Override
	public void setResultsType(Tuple resultsType) {
		this.resultsType = resultsType;
	}

	@Override
	public List<Object> getResults() {
		return results;
	}

	@Override
	public void addResult(Object value) {
		results.add(value);
	}

	@Override
	public void bindComplete() {
	}

	@Override
	public void closeComplete() {
	}

	@Override
	public void commandComplete(String commandTag) {
		completionTag = commandTag;
	}

	@Override
	public void error(Error error) {
		this.error = error;
	}

}
