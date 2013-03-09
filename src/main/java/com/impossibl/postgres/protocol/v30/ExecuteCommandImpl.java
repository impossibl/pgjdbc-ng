package com.impossibl.postgres.protocol.v30;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.impossibl.postgres.protocol.Error;
import com.impossibl.postgres.protocol.ExecuteCommand;

public class ExecuteCommandImpl extends CommandImpl implements ExecuteCommand {

	String command;
	String resultCommand;
	Long resultRowsAffected;
	Long resultInsertedOid;

	private ProtocolHandler handler = new AbstractProtocolHandler() {
		
		@Override
		public boolean isComplete() {
			return resultCommand  != null || error != null;
		}
	
		@Override
		public void commandComplete(String command, Long rowsAffected, Long oid) {
			ExecuteCommandImpl.this.resultCommand = command;
			ExecuteCommandImpl.this.resultRowsAffected = rowsAffected;
			ExecuteCommandImpl.this.resultInsertedOid = oid;
		}

		@Override
		public void error(Error error) {
			ExecuteCommandImpl.this.error = error;
		}
		
	};
	
	
	public ExecuteCommandImpl(String command) {
		this.command = command;
	}
	
	@Override
	public String getResultCommand() {
		return resultCommand;
	}

	@Override
	public Long getResultRowsAffected() {
		return resultRowsAffected;
	}

	@Override
	public List<Long> getResultInsertedOids() {
		return Arrays.asList(resultInsertedOid);
	}

	public void execute(ProtocolImpl protocol) throws IOException {
		
		protocol.sendQuery(command);
		
		protocol.run(handler);
	}

}
