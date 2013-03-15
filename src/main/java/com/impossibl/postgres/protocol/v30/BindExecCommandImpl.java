package com.impossibl.postgres.protocol.v30;

import static com.impossibl.postgres.protocol.ServerObjectType.Portal;
import static com.impossibl.postgres.system.Settings.FIELD_VARYING_LENGTH_MAX;
import static com.impossibl.postgres.utils.Factory.createInstance;
import static java.util.Arrays.asList;
import static org.apache.commons.beanutils.BeanUtils.setProperty;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.mapper.Mapper;
import com.impossibl.postgres.mapper.PropertySetter;
import com.impossibl.postgres.protocol.BindExecCommand;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.SettingsContext;
import com.impossibl.postgres.system.procs.Arrays;
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
		public synchronized void emptyQuery() {
			status = Status.Completed;
			notifyAll();
		}

		@Override
		public synchronized void portalSuspended() {
			status = Status.Suspended;
			notifyAll();
		}

		@Override
		public synchronized void commandComplete(String command, Long rowsAffected, Long oid) {
			status = Status.Completed;
			BindExecCommandImpl.this.resultCommand = command;
			BindExecCommandImpl.this.resultRowsAffected = rowsAffected;
			BindExecCommandImpl.this.resultInsertedOid = oid;
			notifyAll();
		}

		@Override
		public synchronized void error(Notice error) {
			BindExecCommandImpl.this.error = error;
			notifyAll();
		}

		@Override
		public void notice(Notice notice) {
			addNotice(notice);
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
		
		// Setup context for parsing fields with customized parameters
		//
		parsingContext = new SettingsContext(protocol.getContext());
		parsingContext.setSetting(FIELD_VARYING_LENGTH_MAX, maxFieldLength);

		BindExecCommandListener listener = new BindExecCommandListener(parsingContext);
		
		protocol.setListener(listener);

		if(status != Status.Suspended) {

			protocol.sendBind(portalName, statementName, parameterTypes, parameterValues);

		}

		if(resultFields == null) {

			protocol.sendDescribe(Portal, portalName);

		}

		protocol.sendExecute(portalName, maxRows);

		protocol.sendFlush();

		reset();

		waitFor(listener);

		if(status == Status.Completed) {

			protocol.sendSync();

		}

	}

	@SuppressWarnings("unchecked")
	protected void setField(Object instance, int idx, String name, Object value) throws IOException {

		if (Arrays.is(instance)) {

			Arrays.set(instance, idx, value);
			return;
		}
		else if (instance instanceof Map) {

			((Map<Object, Object>) instance).put(name, value);
			return;
		}
		else {

			try {

				java.lang.reflect.Field field;

				if ((field = instance.getClass().getField(name)) != null) {
					field.set(instance, value);
					return;
				}

			}
			catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e1) {

				try {
					setProperty(instance, name.toString(), value);
					return;
				}
				catch (IllegalAccessException | InvocationTargetException e) {
				}

			}
		}

		throw new IllegalStateException("invalid poperty name/index");
	}

}

