package com.impossibl.postgres.protocol.v30;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.impossibl.postgres.protocol.Error;
import com.impossibl.postgres.protocol.QueryCommand;



public class QueryCommandImpl extends CommandImpl implements QueryCommand {

	String command;
	String resultCommand;
	Long resultRowsAffected;
	Long resultInsertedOid;

	private ProtocolListener listener = new BaseProtocolListener() {

		@Override
		public boolean isComplete() {
			return resultCommand != null || error != null;
		}

		@Override
		public void commandComplete(String command, Long rowsAffected, Long oid) {
			QueryCommandImpl.this.resultCommand = command;
			QueryCommandImpl.this.resultRowsAffected = rowsAffected;
			QueryCommandImpl.this.resultInsertedOid = oid;
		}

		@Override
		public void error(Error error) {
			QueryCommandImpl.this.error = error;
		}

	};

	public QueryCommandImpl(String command) {
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

		protocol.setListener(listener);

		protocol.sendQuery(command);

		waitFor(listener);
	}

}
