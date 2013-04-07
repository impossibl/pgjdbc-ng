package com.impossibl.postgres.protocol.v30;

import static com.google.common.collect.Lists.newArrayList;
import static com.impossibl.postgres.protocol.ServerObjectType.Portal;
import static com.impossibl.postgres.system.Settings.FIELD_VARYING_LENGTH_MAX;
import static com.impossibl.postgres.utils.Factory.createInstance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.impossibl.postgres.mapper.Mapper;
import com.impossibl.postgres.mapper.PropertySetter;
import com.impossibl.postgres.protocol.BindExecCommand;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.SettingsContext;
import com.impossibl.postgres.types.Type;



public class BindExecCommandImpl extends CommandImpl implements BindExecCommand {
	
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
		public void rowDescription(List<ResultField> newResultFields) {
			resultFields = newResultFields;
			resultFieldFormats = getResultFieldFormats(newResultFields);
			resultBatch.fields = newResultFields;
			resultBatch.results = (resultFields != null && !resultFields.isEmpty()) ? new ArrayList<>() : null;
			resultSetters = Mapper.buildMapping(rowType, newResultFields);
		}

		@Override
		public void noData() {
			resultBatch.fields = Collections.emptyList();
			resultBatch.results = null;
		}

		@Override
		public void rowData(ChannelBuffer buffer) throws IOException {

			int itemCount = buffer.readShort();

			Object rowInstance = createInstance(rowType, itemCount);

			for (int c = 0; c < itemCount; ++c) {

				ResultField field = resultBatch.fields.get(c);

				Type fieldType = field.getType();
				
				Type.Codec.Decoder decoder = fieldType.getCodec(field.format).decoder;
				
				Object fieldVal = decoder.decode(fieldType, buffer, context);

				resultSetters.get(c).set(rowInstance, fieldVal);
			}

			@SuppressWarnings("unchecked")
			List<Object> res = (List<Object>) resultBatch.results;
			res.add(rowInstance);
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
		public synchronized void commandComplete(String command, Long rowsAffected, Long oid) {
			status = Status.Completed;
			resultBatch.command = command;
			resultBatch.rowsAffected = rowsAffected;
			resultBatch.insertedOid = oid;
			
			if(maxRows > 0) {
				notifyAll();
			}
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
	private List<PropertySetter> resultSetters;
	private int maxRows;
	private int maxFieldLength;
	private Status status;
	private SettingsContext parsingContext;
	private ResultBatch resultBatch;
	private List<Format> resultFieldFormats;
	
	
	
	public BindExecCommandImpl(String portalName, String statementName, List<Type> parameterTypes, List<Object> parameterValues, List<ResultField> resultFields, Class<?> rowType) {

		this.statementName = statementName;
		this.portalName = portalName;
		this.parameterTypes = parameterTypes;
		this.parameterValues = parameterValues;
		this.resultFields = resultFields;
		this.rowType = rowType;
		this.maxRows = 0;
		this.maxFieldLength = Integer.MAX_VALUE;
		
		if(resultFields != null) {
			this.resultSetters = Mapper.buildMapping(rowType, resultFields);
			this.resultFieldFormats = getResultFieldFormats(resultFields);
		}
		else {
			this.resultFieldFormats = Collections.emptyList();
		}
		
	}

	public void reset() {
		status = null;
		resultBatch = new ResultBatch();
		resultBatch.fields = resultFields;
		resultBatch.results = (resultFields != null && !resultFields.isEmpty()) ? new ArrayList<>() : null;
	}

	@Override
	public String getStatementName() {
		return statementName;
	}

	@Override
	public String getPortalName() {
		return portalName;
	}

	@Override
	public Status getStatus() {
		return status;
	}

	@Override
	public List<Type> getParameterTypes() {
		return parameterTypes;
	}

	@Override
	public void setParameterTypes(List<Type> parameterTypes) {
		this.parameterTypes = parameterTypes;
	}

	@Override
	public List<Object> getParameterValues() {
		return parameterValues;
	}

	@Override
	public void setParameterValues(List<Object> parameterValues) {
		this.parameterValues = parameterValues;
	}

	@Override
	public int getMaxRows() {
		return maxRows;
	}

	@Override
	public void setMaxRows(int maxRows) {
		this.maxRows = maxRows;
	}

	@Override
	public int getMaxFieldLength() {
		return maxFieldLength;
	}

	@Override
	public void setMaxFieldLength(int maxFieldLength) {
		this.maxFieldLength = maxFieldLength;
	}

	@Override
	public List<ResultBatch> getResultBatches() {
		return newArrayList(resultBatch);
	}

	public void execute(ProtocolImpl protocol) throws IOException {
		
		// Setup context for parsing fields with customized parameters
		//
		parsingContext = new SettingsContext(protocol.getContext());
		parsingContext.setSetting(FIELD_VARYING_LENGTH_MAX, maxFieldLength);

		BindExecCommandListener listener = new BindExecCommandListener(parsingContext);
		
		protocol.setListener(listener);
		
		ChannelBuffer msg = ChannelBuffers.dynamicBuffer();

		if(status != Status.Suspended) {
			
			protocol.writeBind(msg, portalName, statementName, parameterTypes, parameterValues, resultFieldFormats);

		}

		reset();

		if(resultFields == null) {

			protocol.writeDescribe(msg, Portal, portalName);

		}

		protocol.writeExecute(msg, portalName, maxRows);

		if(maxRows > 0 && protocol.getTransactionStatus() == TransactionStatus.Idle) {
			protocol.writeFlush(msg);			
		}
		else {
			protocol.writeSync(msg);			
		}
		
		protocol.send(msg);

		waitFor(listener);
		
	}
	
	static List<Format> getResultFieldFormats(List<ResultField> resultFields) {
		
		List<Format> resultFieldFormats = new ArrayList<>();
		
		for(ResultField resultField : resultFields) {
			resultField.format = resultField.getType().getResultFormat();
			resultFieldFormats.add(resultField.format);
		}
		
		return resultFieldFormats;
	}

}
