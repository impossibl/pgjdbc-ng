package com.impossibl.postgres.protocol;

import java.util.List;

public interface QueryCommand extends Command {

	String getResultCommand();
	Long getResultRowsAffected();
	List<Long> getResultInsertedOids();
	List<?> getResults();
	
}
