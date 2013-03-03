package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.utils.DataInputStream;

public class NotificationResponseMP implements MessageProcessor {

	@Override
	public void process(DataInputStream in, ResponseHandler handler) throws IOException {

		int processId = in.readInt();
		String channelName = in.readCString();
		String payload = in.readCString();
		
		handler.getContext().reportNotification(processId, channelName, payload);
		
	}

}
