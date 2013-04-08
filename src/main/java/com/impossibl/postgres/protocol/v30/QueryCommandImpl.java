package com.impossibl.postgres.protocol.v30;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.QueryCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.TransactionStatus;
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
			return !resultBatches.isEmpty() || error != null;
		}

		@Override
		public void rowDescription(List<ResultField> resultFields) {
			resultBatch.fields = resultFields;
			resultBatch.results = !resultFields.isEmpty() ? new ArrayList<>() : null;
		}

		@Override
		public void rowData(ChannelBuffer buffer) throws IOException {
						
			int fieldCount = buffer.readShort();

			Object[] rowInstance = new Object[fieldCount];

			for (int c = 0; c < fieldCount; ++c) {

				ResultField field = resultBatch.fields.get(c);

				Type fieldType = field.typeRef.get();
				
				Type.Codec.Decoder decoder = fieldType.getCodec(field.format).decoder;
				
				Object fieldVal = decoder.decode(fieldType, buffer, context);

				rowInstance[c] = fieldVal;
			}

			@SuppressWarnings("unchecked")
			List<Object> res = (List<Object>) resultBatch.results;
			res.add(rowInstance);
		}

		@Override
		public void commandComplete(String command, Long rowsAffected, Long oid) {
			resultBatch.command = command;
			resultBatch.rowsAffected = rowsAffected;
			resultBatch.insertedOid = oid;
			
			resultBatches.add(resultBatch);
			resultBatch = new ResultBatch();
		}

		@Override
		public void error(Notice error) {
			QueryCommandImpl.this.error = error;
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


	
	String command;
	List<ResultBatch> resultBatches;
	ResultBatch resultBatch;

	
	
	public QueryCommandImpl(String command) {
		this.command = command;
	}

	@Override
	public List<ResultBatch> getResultBatches() {
		return resultBatches;
	}

	public void execute(ProtocolImpl protocol) throws IOException {
		
		resultBatch = new ResultBatch();
		resultBatches = new ArrayList<>();

		QueryListener listener = new QueryListener(protocol.getContext());
		
		protocol.setListener(listener);

		ChannelBuffer msg = ChannelBuffers.dynamicBuffer();
		
		protocol.writeQuery(msg, command);
		
		protocol.writeSync(msg);

		protocol.send(msg);

		waitFor(listener);
	}

	@Override
	public Status getStatus() {
		return Status.Completed;
	}

	@Override
	public int getMaxFieldLength() {
		return 0;
	}

	@Override
	public void setMaxFieldLength(int maxFieldLength) {
	}

	@Override
	public int getMaxRows() {
		return 0;
	}

	@Override
	public void setMaxRows(int maxRows) {
	}

}
