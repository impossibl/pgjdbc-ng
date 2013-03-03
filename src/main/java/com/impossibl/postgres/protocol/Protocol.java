package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.types.Type;

public interface Protocol {

	void dispatch(ResponseHandler handler) throws IOException;

	void startup(Map<String, Object> params) throws IOException;
	void authenticate(String password) throws IOException;

	void query(String query) throws IOException;

	void queryParse(String stmtName, String query, List<Type> paramTypes) throws IOException;
	void queryBind(String portalName, String stmtName, List<Type> paramTypes, List<Object> paramValues) throws IOException;
	void queryExecute(String portalName, int maxRows) throws IOException;
	
	void describe(char targetType, String targetName) throws IOException;

	void close(char targetType, String targetName) throws IOException;

	void functionCall(int functionId, List<Type> paramTypes, List<Object> paramValues) throws IOException;
	
	void sync() throws IOException;
	void flush() throws IOException;
	
	void terminate() throws IOException;

}
