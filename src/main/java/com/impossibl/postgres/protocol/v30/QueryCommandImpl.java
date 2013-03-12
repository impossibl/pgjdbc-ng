package com.impossibl.postgres.protocol.v30;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.QueryCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;



public class QueryCommandImpl extends CommandImpl implements QueryCommand {

	class QueryListener extends BaseProtocolListener {

		Context context;
	
		public QueryListener(Context context) {
			super();
			this.context = context;
		}

		@Override
		public boolean isComplete() {
			return resultCommand != null || error != null;
		}

		@Override
		public void rowDescription(List<ResultField> resultFields) {
			QueryCommandImpl.this.resultFields = resultFields;
		}

		@Override
		public void rowData(ChannelBuffer buffer) throws IOException {
			
			int fieldCount = buffer.readShort();

			Object[] rowInstance = new Object[fieldCount];

			for (int c = 0; c < fieldCount; ++c) {

				ResultField field = resultFields.get(c);

				Type fieldType = field.type;
				Object fieldVal = null;

				switch (field.format) {
				case Text:
					fieldVal = fieldType.getTextCodec().decoder.decode(fieldType, buffer, context);
					break;

				default:
					throw new IOException("simple queries only support text format");
				}

				rowInstance[c] = fieldVal;
			}

			results.add(rowInstance);
		}

		@Override
		public synchronized void commandComplete(String command, Long rowsAffected, Long oid) {
			QueryCommandImpl.this.resultCommand = command;
			QueryCommandImpl.this.resultRowsAffected = rowsAffected;
			QueryCommandImpl.this.resultInsertedOid = oid;
			notifyAll();
		}

		@Override
		public synchronized void error(Notice error) {
			QueryCommandImpl.this.error = error;
			notifyAll();
		}

		@Override
		public void notice(Notice notice) {
			addNotice(notice);
		}

	};


	
	String command;
	String resultCommand;
	Long resultRowsAffected;
	Long resultInsertedOid;
	List<ResultField> resultFields;
	List<Object[]> results;

	public QueryCommandImpl(String command) {
		this.command = command;
	}

	@Override
	public String getResultCommand() {
		return resultCommand;
	}

	@Override
	public Long getResultRowsAffected() {
		return resultRowsAffected;
	}

	@Override
	public List<Long> getResultInsertedOids() {
		return Arrays.asList(resultInsertedOid);
	}

	@Override
	public List<Object[]> getResults() {
		return results;
	}

	public void execute(ProtocolImpl protocol) throws IOException {
		
		results = new ArrayList<>();

		QueryListener listener = new QueryListener(protocol.getContext());
		
		protocol.setListener(listener);

		protocol.sendQuery(command);

		waitFor(listener);
	}

}
