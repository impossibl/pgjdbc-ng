package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.system.Context;

public class ExecuteCommand extends Command {

	String command;
	boolean complete;

	private ProtocolHandler handler = new AbstractProtocolHandler() {
		
		@Override
		public boolean isComplete() {
			return complete || error != null;
		}
	
		@Override
		public void commandComplete(String command, Integer rowsAffected, Integer oid) {
			ExecuteCommand.this.complete = true;
		}

		@Override
		public void error(Error error) {
			ExecuteCommand.this.error = error;
		}
		
	};
	
	
	public ExecuteCommand(String command) {
		this.command = command;
		this.complete = false;
	}
	
	public void execute(Context context) throws IOException {
		
		try(Protocol protocol = context.lockProtocol()) {
			
			protocol.sendQuery(command);
			
			protocol.run(handler);
		}
		
	}

}
