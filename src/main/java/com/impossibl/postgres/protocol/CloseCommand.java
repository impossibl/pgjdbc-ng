package com.impossibl.postgres.protocol;

public interface CloseCommand extends Command {
	
	ServerObject getTarget();

	String getTargetName();

}
