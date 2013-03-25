package com.impossibl.postgres.protocol.v30;

import java.io.IOException;
import java.util.List;

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.FunctionCallCommand;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.types.Type;



public class FunctionCallCommandImpl extends CommandImpl implements FunctionCallCommand {

	private String functionName;
	private List<Type> parameterTypes;
	private List<Object> parameterValues;
	private Object result;
	private ProtocolListener listener = new BaseProtocolListener() {

		@Override
		public boolean isComplete() {
			return result != null || error != null;
		}

		@Override
		public void functionResult(Object value) {
			FunctionCallCommandImpl.this.result = value;
		}

		@Override
		public void error(Notice error) {
			FunctionCallCommandImpl.this.error = error;
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

	public FunctionCallCommandImpl(String functionName, List<Type> parameterTypes, List<Object> parameterValues) {

		this.functionName = functionName;
		this.parameterTypes = parameterTypes;
		this.parameterValues = parameterValues;
	}

	@Override
	public String getFunctionName() {
		return functionName;
	}

	public List<Type> getParameterTypes() {
		return parameterTypes;
	}

	public List<Object> getParameterValues() {
		return parameterValues;
	}

	@Override
	public Object getResult() {
		return result;
	}

	public void execute(ProtocolImpl protocol) throws IOException {

		protocol.setListener(listener);

		int procId = protocol.getContext().getRegistry().lookupProcId(functionName);
		if(procId == 0)
			throw new IOException("invalid function name");

		protocol.sendFunctionCall(procId, parameterTypes, parameterValues);
		
		protocol.sendSync();

		waitFor(listener);

	}

}
