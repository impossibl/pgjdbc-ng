package com.impossibl.postgres.protocol.v30;

import static com.impossibl.postgres.protocol.ServerObject.Statement;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.impossibl.postgres.protocol.Error;
import com.impossibl.postgres.protocol.PrepareCommand;
import com.impossibl.postgres.types.Type;

public class PrepareCommandImpl extends CommandImpl implements PrepareCommand {

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
			PrepareCommandImpl.this.describedParameterTypes = parameterTypes;
		}
	
		@Override
		public void noData() {
			PrepareCommandImpl.this.describedParameterTypes = Collections.emptyList();
		}
	
		@Override
		public void error(Error error) {
			PrepareCommandImpl.this.error = error;
		}
		
	};
	
	
	public PrepareCommandImpl(String statementName, String query, List<Type> parseParameterTypes) {
		this.statementName = statementName;
		this.query = query;
		this.parseParameterTypes = parseParameterTypes;
	}
	
	public String getQuery() {
		return query;
	}


	@Override
	public String getStatementName() {
		return statementName;
	}

	@Override
	public List<Type> getParseParameterTypes() {
		return parseParameterTypes;
	}

	public List<Type> getDescribedParameterTypes() {
		return describedParameterTypes;
	}

	public void execute(ProtocolImpl protocol) throws IOException {

		protocol.sendParse(statementName, query, parseParameterTypes);
		
		protocol.sendDescribe(Statement, statementName);
		
		protocol.sendFlush();
		
		protocol.run(handler);
		
	}
	
}
