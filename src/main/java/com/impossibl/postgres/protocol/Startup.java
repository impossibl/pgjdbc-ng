package com.impossibl.postgres.protocol;

import com.impossibl.postgres.Context;

public class Startup extends BaseResponseHandler {
	
	boolean complete;

	public Startup(Context context) {
		super(context);
	}

	public boolean isComplete() {
		return complete;
	}
	
	public void ready(TransactionStatus txStatus) {
		complete = true;
	}

	@Override
	public void error(Error error) {
		super.error(error);
		complete = false;
	}

}
