package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.types.Type;


public interface Protocol {
	
	TransactionStatus getTransactionStatus();

	StartupCommand createStartup(Map<String,Object> settings);
	PrepareCommand createPrepare(String statementName, String sqlText, List<Type> parameterTypes);
	QueryCommand createQuery(String portalName, String statementName, List<Type> parameterTypes, List<Object> parameterValues, Class<?> rowType);
	ExecuteCommand createExec(String sqlText);
	
	void execute(Command cmd) throws IOException;

}
