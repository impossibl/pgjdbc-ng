package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.types.Type;


public interface Protocol {
	
	TransactionStatus getTransactionStatus();

	StartupCommand createStartup(Map<String,Object> parameters);
	PrepareCommand createPrepare(String statementName, String sqlText, List<Type> parameterTypes);
	BindExecCommand createBindExec(String portalName, String statementName, List<Type> parameterTypes, List<Object> parameterValues, List<ResultField> resultFields, Class<?> rowType);
	QueryCommand createQuery(String sqlText);
	FunctionCallCommand createFunctionCall(String functionName, List<Type> parameterTypes, List<Object> parameterValues);
	
	CloseCommand createClose(ServerObjectType objectType, String objectName);
	
	void execute(Command cmd) throws IOException;
	
	void shutdown();


}
