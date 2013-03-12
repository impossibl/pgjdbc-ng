package com.impossibl.postgres.protocol.v30;

import static com.impossibl.postgres.protocol.ServerObject.Statement;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.PrepareCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.types.Type;



public class PrepareCommandImpl extends CommandImpl implements PrepareCommand {

	private String statementName;
	private String query;
	private List<Type> parseParameterTypes;
	private List<Type> describedParameterTypes;
	private List<ResultField> describedResultFields;
	private ProtocolListener listener = new BaseProtocolListener() {

		@Override
		public void parseComplete() {
		}

		@Override
		public boolean isComplete() {
			return describedResultFields != null || error != null;
		}

		@Override
		public synchronized void parametersDescription(List<Type> parameterTypes) {
			PrepareCommandImpl.this.describedParameterTypes = parameterTypes;
			notifyAll();
		}

		@Override
		public synchronized void rowDescription(List<ResultField> resultFields) {

			// Ensure we are working with binary fields
			for(ResultField field : resultFields)
				field.format = Format.Binary;

			PrepareCommandImpl.this.describedResultFields = resultFields;
			notifyAll();
		}

		@Override
		public synchronized void noData() {
			PrepareCommandImpl.this.describedResultFields = Collections.emptyList();
			notifyAll();
		}

		@Override
		public synchronized void error(Notice error) {
			PrepareCommandImpl.this.error = error;
			notifyAll();
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

	@Override
	public List<Type> getDescribedParameterTypes() {
		return describedParameterTypes;
	}

	@Override
	public List<ResultField> getDescribedResultFields() {
		return describedResultFields;
	}

	@Override
	public void execute(ProtocolImpl protocol) throws IOException {

		protocol.setListener(listener);

		protocol.sendParse(statementName, query, parseParameterTypes);

		protocol.sendDescribe(Statement, statementName);

		protocol.sendFlush();

		waitFor(listener);

	}

}
