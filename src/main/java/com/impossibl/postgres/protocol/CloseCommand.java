package com.impossibl.postgres.protocol;

public interface CloseCommand extends Command {
	
	ServerObjectType getObjectType();

	long getObjectId();

}
