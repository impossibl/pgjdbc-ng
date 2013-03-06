package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.Context;

public abstract class Command {
	
	protected Error error;
	
	public Error getError() {
		return error;
	}

	public void setError(Error error) {
		this.error = error;
	}

	public abstract void execute(Context context) throws IOException;

}
