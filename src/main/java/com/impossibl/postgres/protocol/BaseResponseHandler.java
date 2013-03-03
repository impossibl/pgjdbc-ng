package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.util.List;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Tuple;
import com.impossibl.postgres.types.Type;

public abstract class BaseResponseHandler implements ResponseHandler {
	
	Context context;
	Error error;
	
	public BaseResponseHandler(Context context) {
		super();
		this.context = context;
	}

	@Override
	public Context getContext() {
		return context;
	}
	
	@Override
	public Error getError() {
		return error;
	}

	@Override
	public List<Type> getParameterTypes() {
		throw new IllegalStateException();
	}

	@Override
	public void setParameterTypes(List<Type> asList) {
		throw new IllegalStateException();
	}

	@Override
	public Tuple getResultsType() {
		throw new IllegalStateException();
	}

	@Override
	public void setResultsType(Tuple tuple) {
		throw new IllegalStateException();
	}

	@Override
	public List<Object> getResults() {
		throw new IllegalStateException();
	}

	@Override
	public void addResult(Object value) {
		throw new IllegalStateException();
	}

	@Override
	public void ready(TransactionStatus txStatus) {
	}

	@Override
	public void bindComplete() {
		throw new IllegalStateException();
	}

	@Override
	public void closeComplete() {
		throw new IllegalStateException();
	}

	@Override
	public void commandComplete(String commandTag) {
		throw new IllegalStateException();
	}

	@Override
	public void error(Error error) {
		this.error = error;
		try {
			context.getProtocol().sync();
		}
		catch (IOException e) {
			//TODO: catch
		}
	}

}
