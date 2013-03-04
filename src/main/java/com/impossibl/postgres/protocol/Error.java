package com.impossibl.postgres.protocol;

public class Error {
	
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

