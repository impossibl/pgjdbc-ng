package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.utils.DataInputStream;

public class ParameterStatusMP implements MessageProcessor {

	@Override
	public void process(DataInputStream in, ResponseHandler handler) throws IOException {
		
		String name = in.readCString();
		String value = in.readCString();
		
		handler.getContext().updateSystemParameter(name, value);
		
	}

}
