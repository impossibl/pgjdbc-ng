package com.impossibl.postgres.protocol;

import static com.impossibl.postgres.protocol.ServerObject.Portal;

import java.io.IOException;
import java.util.List;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;

public class PrepareCommand extends Command {

	private String statementName;
	private String query;
	private List<Type> parameterTypes;
	
	public PrepareCommand(String statementName, String query, List<Type> parameterTypes) {
		this.statementName = statementName;
		this.query = query;
		this.parameterTypes = parameterTypes;
	}
	
	public String getQuery() {
		return query;
	}

	public List<Type> getParameterTypes() {
		return parameterTypes;
	}

	public void execute(Context context) {
		
		ProtocolHandler handler = new AbstractProtocolHandler() {
			
			@Override
			public void parseComplete() {
			}
			
			@Override
			public boolean isComplete() {
				return parameterTypes != null || error != null;
			}
		
			@Override
			public void parametersDescription(List<Type> parameterTypes) {
				PrepareCommand.this.parameterTypes = parameterTypes;
			}
		
			@Override
			public void noData() {
			}
		
			@Override
			public void error(Error error) {
				PrepareCommand.this.error = error;
			}
			
		};

		try(ProtocolV30 protocol = context.lockProtocol(handler)) {
			
			protocol.sendParse(statementName, query, parameterTypes);
			
			protocol.sendDescribe(Portal, statementName);
			
			protocol.run();
			
		}
		catch(IOException e) {	
		}
		
	}
	
}
