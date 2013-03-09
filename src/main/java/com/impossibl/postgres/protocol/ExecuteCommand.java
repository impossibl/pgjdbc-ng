package com.impossibl.postgres.protocol;

import java.util.List;

public interface ExecuteCommand extends Command {

	String getResultCommand();
	Long getResultRowsAffected();
	List<Long> getResultInsertedOids();
	
}
