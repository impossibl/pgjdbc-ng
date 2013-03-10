package com.impossibl.postgres.protocol;

import java.util.List;

import com.impossibl.postgres.types.Type;

public interface PrepareCommand extends Command {

	String getStatementName();
	List<Type> getParseParameterTypes();
	List<Type> getDescribedParameterTypes();
	List<ResultField> getDescribedResultFields();
	
}
