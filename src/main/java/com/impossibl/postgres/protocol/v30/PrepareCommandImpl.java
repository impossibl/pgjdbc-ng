package com.impossibl.postgres.protocol.v30;

import static com.impossibl.postgres.protocol.ServerObjectType.Statement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.PrepareCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.protocol.TypeRef;
import com.impossibl.postgres.types.Type;



public class PrepareCommandImpl extends CommandImpl implements PrepareCommand {

	private String statementName;
	private String query;
	private List<Type> parseParameterTypes;
	private List<TypeRef> describedParameterTypes;
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
		public void parametersDescription(List<TypeRef> parameterTypes) {
			PrepareCommandImpl.this.describedParameterTypes = parameterTypes;
		}

		@Override
		public void rowDescription(List<ResultField> resultFields) {

			// Ensure we are working with binary fields
			for(ResultField field : resultFields)
				field.format = Format.Binary;

			PrepareCommandImpl.this.describedResultFields = resultFields;
		}

		@Override
		public void noData() {
			PrepareCommandImpl.this.describedResultFields = Collections.emptyList();
		}

		@Override
		public void error(Notice error) {
			PrepareCommandImpl.this.error = error;
		}

		@Override
		public synchronized void ready(TransactionStatus txStatus) {
			notifyAll();
		}

		@Override
		public void notice(Notice notice) {
			addNotice(notice);
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
		List<Type> types = new ArrayList<>();
		for(TypeRef typeRef : describedParameterTypes) {
			types.add(typeRef.get());
		}
		return types;
	}

	@Override
	public List<ResultField> getDescribedResultFields() {
		return describedResultFields;
	}

	@Override
	public void execute(ProtocolImpl protocol) throws IOException {

		protocol.setListener(listener);

		ChannelBuffer msg = ChannelBuffers.dynamicBuffer();
		
		protocol.writeParse(msg, statementName, query, parseParameterTypes);

		protocol.writeDescribe(msg, Statement, statementName);

		protocol.writeSync(msg);

		protocol.send(msg);

		waitFor(listener);
		
	}

}
