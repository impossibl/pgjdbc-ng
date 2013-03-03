package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.utils.DataInputStream;

public class ParameterStatusMP implements MessageProcessor {

	@Override
	public void process(DataInputStream in, Context context) throws IOException {
		
		String name = in.readCString();
		String value = in.readCString();
		
		context.updateSystemParameter(name, value);
		
	}

}
