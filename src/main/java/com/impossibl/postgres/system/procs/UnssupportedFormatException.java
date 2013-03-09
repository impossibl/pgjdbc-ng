package com.impossibl.postgres.system.procs;

import java.io.IOException;

public class UnssupportedFormatException extends IOException {

	private static final long serialVersionUID = -2141246237354641611L;

	public UnssupportedFormatException() {
		super();
	}

	public UnssupportedFormatException(String message, Throwable cause) {
		super(message, cause);
	}

	public UnssupportedFormatException(String message) {
		super(message);
	}

	public UnssupportedFormatException(Throwable cause) {
		super(cause);
	}

}
