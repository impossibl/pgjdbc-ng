package com.impossibl.postgres.protocol;

import java.util.List;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Tuple;
import com.impossibl.postgres.types.Type;

public interface ResponseHandler {
	
	class Error {
		
		enum Severity {
			ERROR,
			FATAL,
			PANIC
		}
		
		public Severity severity;
		public String code;
		public String message;
		public String detail;
		public String hint;
		public int position;
		public String routine;
		public String file;
		public int line;
	}
	
	Context getContext();
	Error getError();
	boolean isComplete();
	
	List<Type> getParameterTypes();
	void setParameterTypes(List<Type> asList);
	
	Tuple getResultsType();
	void setResultsType(Tuple tuple);
	
	List<Object> getResults();
	void addResult(Object value);
	
	void ready(TransactionStatus txStatus);

	void bindComplete();
	void closeComplete();
	void commandComplete(String commandTag);

	void error(Error error);

}
