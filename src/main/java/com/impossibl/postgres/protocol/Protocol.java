package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;

public interface Protocol extends AutoCloseable {

	public void close();
	
	public void setHandler(ProtocolHandler handler);
	
	public void run() throws IOException;
	
	public void sendStartup(Map<String, Object> params) throws IOException;
	public void sendPassword(String password) throws IOException;	
	public void sendQuery(String query) throws IOException;
	public void sendParse(String stmtName, String query, List<Type> paramTypes) throws IOException;
	public void sendBind(String portalName, String stmtName, List<Type> parameterTypes, List<Object> parameterValues) throws IOException;
	public void sendDescribe(ServerObject target, String targetName) throws IOException;
	public void sendExecute(String portalName, int maxRows) throws IOException;
	public void sendFunctionCall(int functionId, List<Type> paramTypes, List<Object> paramValues) throws IOException;
	public void sendClose(ServerObject target, String targetName) throws IOException;
	public void sendFlush() throws IOException;
	public void sendSync() throws IOException;
	public void sendTerminate() throws IOException;

	public Object parseRowData(DataInputStream in, List<ResultField> resultFields, Class<?> rowType) throws IOException;
	public Object parseResultData(DataInputStream in, Type resultType) throws IOException;
	
}
