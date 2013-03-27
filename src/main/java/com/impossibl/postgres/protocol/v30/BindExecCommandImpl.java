package com.impossibl.postgres.protocol.v30;

import static com.impossibl.postgres.protocol.ServerObjectType.Portal;
import static com.impossibl.postgres.system.Settings.FIELD_VARYING_LENGTH_MAX;
import static com.impossibl.postgres.utils.Factory.createInstance;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.mapper.Mapper;
import com.impossibl.postgres.mapper.PropertySetter;
import com.impossibl.postgres.protocol.BindExecCommand;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.SettingsContext;
import com.impossibl.postgres.types.Type;



public class BindExecCommandImpl extends CommandImpl implements BindExecCommand {

	public enum Status {
		Completed,
		Suspended
	}

	class BindExecCommandListener extends BaseProtocolListener {
		
		Context context;
		
		public BindExecCommandListener(Context context) {
			this.context = context;
		}

		@Override
		public boolean isComplete() {
			return status != null || error != null;
		}

		@Override
		public void bindComplete() {
		}

		@Override
		public void rowDescription(List<ResultField> resultFields) {
			BindExecCommandImpl.this.resultFields = resultFields;
			BindExecCommandImpl.this.resultSetters = Mapper.buildMapping(rowType, resultFields);
		}

		@Override
		public void noData() {
			resultFields = Collections.emptyList();
		}

		@Override
		public void rowData(ChannelBuffer buffer) throws IOException {

			int itemCount = buffer.readShort();

			Object rowInstance = createInstance(rowType, itemCount);

			for (int c = 0; c < itemCount; ++c) {

				ResultField field = resultFields.get(c);

				Type fieldType = field.type;
				Object fieldVal = null;

				switch (field.format) {
				case Text:
					fieldVal = fieldType.getTextCodec().decoder.decode(fieldType, buffer, parsingContext);
					break;

				case Binary:
					fieldVal = fieldType.getBinaryCodec().decoder.decode(fieldType, buffer, parsingContext);
					break;
				}

				resultSetters.get(c).set(rowInstance, fieldVal);
			}

			results.add(rowInstance);
		}

		@Override
		public void emptyQuery() {
			status = Status.Completed;
		}

		@Override
		public synchronized void portalSuspended() {
			status = Status.Suspended;
			notifyAll();
		}

		@Override
		public void commandComplete(String command, Long rowsAffected, Long oid) {
			status = Status.Completed;
			BindExecCommandImpl.this.resultCommand = command;
			BindExecCommandImpl.this.resultRowsAffected = rowsAffected;
			BindExecCommandImpl.this.resultInsertedOid = oid;
		}

		@Override
		public void error(Notice error) {
			BindExecCommandImpl.this.error = error;
		}

		@Override
		public void notice(Notice notice) {
			addNotice(notice);
		}

		@Override
		public synchronized void ready(TransactionStatus txStatus) {
			notifyAll();
		}

	};


	private String statementName;
	private String portalName;
	private List<Type> parameterTypes;
	private List<Object> parameterValues;
	private List<ResultField> resultFields;
	private Class<?> rowType;
	private List<Object> results;
	private List<PropertySetter> resultSetters;
	private String resultCommand;
	private Long resultRowsAffected;
	private Long resultInsertedOid;
	private int maxRows;
	private int maxFieldLength;
	private Status status;
	private SettingsContext parsingContext; 

	
	public BindExecCommandImpl(String portalName, String statementName, List<Type> parameterTypes, List<Object> parameterValues, List<ResultField> resultFields, Class<?> rowType) {

		this.statementName = statementName;
		this.portalName = portalName;
		this.parameterTypes = parameterTypes;
		this.parameterValues = parameterValues;
		this.resultFields = resultFields;
		this.rowType = rowType;
		this.results = new ArrayList<>();
		this.maxRows = 0;
		this.maxFieldLength = Integer.MAX_VALUE;
		
		if(resultFields != null)
			this.resultSetters = Mapper.buildMapping(rowType, resultFields);
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

	public int getMaxFieldLength() {
		return maxFieldLength;
	}

	public void setMaxFieldLength(int maxFieldLength) {
		this.maxFieldLength = maxFieldLength;
	}

	public List<ResultField> getResultFields() {
		return resultFields;
	}

	public List<?> getResults() {
		return results;
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> getResults(Class<T> rowType) {
		//TODO do this unchecked
		return (List<T>) results;
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
		
		reset();

		// Setup context for parsing fields with customized parameters
		//
		parsingContext = new SettingsContext(protocol.getContext());
		parsingContext.setSetting(FIELD_VARYING_LENGTH_MAX, maxFieldLength);

		BindExecCommandListener listener = new BindExecCommandListener(parsingContext);
		
		protocol.setListener(listener);

		if(status != Status.Suspended) {

			protocol.sendBind(portalName, statementName, parameterTypes, parameterValues);

		}

		if(resultFields == null || !parameterTypes.isEmpty()) {

			protocol.sendDescribe(Portal, portalName);

		}

		protocol.sendExecute(portalName, maxRows);

		if(maxRows > 0 && protocol.getTransactionStatus() == TransactionStatus.Idle) {
			protocol.sendFlush();			
		}
		else {
			protocol.sendSync();			
		}

		waitFor(listener);
		
	}

}
