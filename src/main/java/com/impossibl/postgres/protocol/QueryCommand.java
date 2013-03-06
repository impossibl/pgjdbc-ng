package com.impossibl.postgres.protocol;

import static com.impossibl.postgres.protocol.ServerObject.Portal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;

public class QueryCommand extends Command {

	public enum Status {
		Completed,
		Suspended
	}

	private String statementName;
	private String portalName;
	private List<Type> parameterTypes;
	private List<Object> parameterValues;
	private List<ResultField> resultFields;
	private Class<?> rowType;
	private List<Object> results;
	private int maxRows;
	private Status status;
	private ProtocolHandler handler = new AbstractProtocolHandler() {
		
		@Override
		public boolean isComplete() {
			return status != null || error != null;
		}
	
		@Override
		public void bindComplete() {
		}

		@Override
		public void rowDescription(List<ResultField> resultFields) {
			QueryCommand.this.resultFields = resultFields;
		}

		@Override
		public void noData() {
			resultFields = Collections.emptyList();
		}
	
		@Override
		public void rowData(Protocol protocol, DataInputStream stream) throws IOException {
			results.add(protocol.parseRowData(stream, resultFields, rowType));
		}

		@Override
		public void emptyQuery() {
			status = Status.Completed;
		}

		@Override
		public void portalSuspended() {
			status = Status.Suspended;
		}

		@Override
		public void commandComplete(String commandTag) {
			status = Status.Completed;
		}

		@Override
		public void error(Error error) {
			QueryCommand.this.error = error;
		}
		
	};
	
	
	public QueryCommand(String portalName, String statementName, List<Type> parameterTypes, List<Object> parameterValues, Class<?> rowType) {
		this.statementName = statementName;
		this.portalName = portalName;
		this.parameterTypes = parameterTypes;
		this.parameterValues = parameterValues;
		this.resultFields = null;
		this.rowType = rowType;
		this.results = new ArrayList<>();
	}
	
	public void reset() {
		status = null;
		results.clear();
	}
	
	public Status getStatus() {
		return status;
	}

public List<Type> getParameterTypes() {
		return parameterTypes;
	}
	
	public int getMaxRows() {
		return maxRows;
	}
	
	public void setMaxRows(int maxRows) {
		this.maxRows = maxRows;
	}
	
	public List<ResultField> getResultFields() {
		return resultFields;
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> getResults(Class<T> type) {
		return (List<T>)results;
	}

	public void execute(Context context) throws IOException {
		
		try(Protocol protocol = context.lockProtocol(handler)) {
			
			if(status != Status.Suspended) {
				
				protocol.sendBind(portalName, statementName, parameterTypes, parameterValues);
			
				protocol.sendDescribe(Portal, portalName);
			
			}
			
			protocol.sendExecute(portalName, maxRows);
			
			protocol.sendFlush();
			
			reset();
			
			protocol.run();
			
			if(status == Status.Completed) {
				
				protocol.sendSync();

			}
			
		}
		
	}

}
