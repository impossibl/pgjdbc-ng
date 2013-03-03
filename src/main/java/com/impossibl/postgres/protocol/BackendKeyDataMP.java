package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.utils.DataInputStream;

public class BackendKeyDataMP implements MessageProcessor {

	@Override
	public void process(DataInputStream in, ResponseHandler handler) throws IOException {

		int processId = in.readInt();
		int secretKey = in.readInt();
		
		handler.getContext().setKeyData(processId, secretKey);
	}

}
