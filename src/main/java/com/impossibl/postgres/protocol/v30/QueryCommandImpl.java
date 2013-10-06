/**
 * Copyright (c) 2013, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
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
