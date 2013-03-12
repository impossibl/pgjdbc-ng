package com.impossibl.postgres.system;

public interface NotificationListener {

	void notification(int processId, String channelName, String payload);
	
}
