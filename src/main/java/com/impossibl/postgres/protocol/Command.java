package com.impossibl.postgres.protocol;

import com.impossibl.postgres.Context;

public abstract class Command {
	
	protected Error error;
	
	public Error getError() {
		return error;
	}

	public void setError(Error error) {
		this.error = error;
	}

	public abstract void execute(Context context);

}
