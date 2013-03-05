package com.impossibl.postgres.protocol;

import java.util.Collections;
import java.util.List;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;

public class PrepareProtocol extends AbstractQueryProtocol {
	
	private List<Type> parameterTypes;
	

	public PrepareProtocol(Context context) {
		super(context);
	}
	
	@Override
	public boolean isRunComplete() {
		return super.isRunComplete() || parameterTypes != null;
	}

	@Override
	protected void parameterDescriptions(List<Type> paramTypes) {
		this.parameterTypes = paramTypes;
	}

	@Override
	protected void parseComplete() {
	}

	@Override
	protected void noData() {
		this.parameterTypes = Collections.emptyList();
	}

	public List<Type> getParameterTypes() {
		return parameterTypes;
	}
	
}
