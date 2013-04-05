package com.impossibl.postgres.protocol;

import java.util.List;

public interface QueryCommand extends Command {

	enum Status {
		Completed,
		Suspended
	}
	
	class ResultBatch {
		public String command;
		public Long rowsAffected;
		public Long insertedOid;
		public List<ResultField> fields;
		public List<?> results;
	}
	
	int getMaxRows();
	void setMaxRows(int maxRows);
	
	int getMaxFieldLength();
	public void setMaxFieldLength(int maxFieldLength);
	
	List<ResultBatch> getResultBatches();
	
	Status getStatus();
	
}
