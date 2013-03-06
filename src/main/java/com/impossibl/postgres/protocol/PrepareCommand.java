package com.impossibl.postgres.protocol;

import static com.impossibl.postgres.protocol.ServerObject.Statement;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;

public class PrepareCommand extends Command {

	private String statementName;
	private String query;
	private List<Type> parseParameterTypes;
	private List<Type> describedParameterTypes;
	private ProtocolHandler handler = new AbstractProtocolHandler() {
		
		@Override
		public void parseComplete() {
		}
		
		@Override
		public boolean isComplete() {
			return describedParameterTypes != null || error != null;
		}
	
		@Override
		public void parametersDescription(List<Type> parameterTypes) {
			PrepareCommand.this.describedParameterTypes = parameterTypes;
		}
	
		@Override
		public void noData() {
			PrepareCommand.this.describedParameterTypes = Collections.emptyList();
		}
	
		@Override
		public void error(Error error) {
			PrepareCommand.this.error = error;
		}
		
	};
	
	
	public PrepareCommand(String statementName, String query, List<Type> parseParameterTypes) {
		this.statementName = statementName;
		this.query = query;
		this.parseParameterTypes = parseParameterTypes;
	}
	
	public String getQuery() {
		return query;
	}

	public List<Type> getDescribedParameterTypes() {
		return describedParameterTypes;
	}

	public void execute(Context context) throws IOException {

		try(Protocol protocol = context.lockProtocol(handler)) {
			
			protocol.sendParse(statementName, query, parseParameterTypes);
			
			protocol.sendDescribe(Statement, statementName);
			
			protocol.sendFlush();
			
			protocol.run();
			
		}
		
	}
	
}
