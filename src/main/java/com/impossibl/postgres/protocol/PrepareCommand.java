package com.impossibl.postgres.protocol;

import java.util.List;

import com.impossibl.postgres.types.Type;

public interface PrepareCommand extends Command {

	long getStatementId();
	List<Type> getParseParameterTypes();
	List<Type> getDescribedParameterTypes();
	List<ResultField> getDescribedResultFields();
	
}
