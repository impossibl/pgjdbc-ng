package com.impossibl.postgres.protocol.v30;

import static com.impossibl.postgres.protocol.ServerObject.Portal;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.impossibl.postgres.protocol.Error;
import com.impossibl.postgres.protocol.QueryCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;

public class QueryCommandImpl extends CommandImpl implements QueryCommand {

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
	private String resultCommand;
	private Long resultRowsAffected;
	private Long resultInsertedOid;
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
			QueryCommandImpl.this.resultFields = resultFields;
		}

		@Override
		public void noData() {
			resultFields = Collections.emptyList();
		}
	
		@Override
		public void rowData(ProtocolImpl protocol, DataInputStream stream) throws IOException {
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
		public void commandComplete(String command, Long rowsAffected, Long oid) {
			status = Status.Completed;
			QueryCommandImpl.this.resultCommand = command;
			QueryCommandImpl.this.resultRowsAffected = rowsAffected;
			QueryCommandImpl.this.resultInsertedOid = oid;
		}

		@Override
		public void error(Error error) {
			QueryCommandImpl.this.error = error;
		}
		
	};
	
	
	public QueryCommandImpl(String portalName, String statementName, List<Type> parameterTypes, List<Object> parameterValues, Class<?> rowType) {
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
	
	@Override
	public String getStatementName() {
		return statementName;
	}

	@Override
	public String getPortalName() {
		return portalName;
	}

	public Status getStatus() {
		return status;
	}

	public List<Type> getParameterTypes() {
		return parameterTypes;
	}
	
	public List<Object> getParameterValues() {
		return parameterValues;
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
	public <T> List<T> getResults(Class<T> rowType) {
		return (List<T>)results;
	}

	public String getResultCommand() {
		return resultCommand;
	}

	public Long getResultRowsAffected() {
		return resultRowsAffected;
	}

	public List<Long> getResultInsertedOids() {
		return asList(resultInsertedOid);
	}

	public void execute(ProtocolImpl protocol) throws IOException {
		
		if(status != Status.Suspended) {
			
			protocol.sendBind(portalName, statementName, parameterTypes, parameterValues);
		
			protocol.sendDescribe(Portal, portalName);
		
		}
		
		protocol.sendExecute(portalName, maxRows);
		
		protocol.sendFlush();
		
		reset();
		
		protocol.run(handler);
		
		if(status == Status.Completed) {
			
			protocol.sendSync();

		}

	}

}
