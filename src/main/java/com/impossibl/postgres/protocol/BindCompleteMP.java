package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.utils.DataInputStream;

public class BindCompleteMP implements MessageProcessor {

	@Override
	public void process(DataInputStream in, ResponseHandler handler) throws IOException {

		handler.bindComplete();
	}

}
