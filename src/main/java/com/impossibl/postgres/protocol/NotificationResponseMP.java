package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.utils.DataInputStream;

public class NotificationResponseMP implements MessageProcessor {

	@Override
	public void process(Protocol proto, DataInputStream in, Context context) throws IOException {

		int processId = in.readInt();
		String channelName = in.readCString();
		String payload = in.readCString();
		
		context.reportNotification(processId, channelName, payload);
		
	}

}
