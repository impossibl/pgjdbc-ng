package com.impossibl.postgres.system.tables;


import com.impossibl.postgres.system.Version;

public interface Table<R> {
	
	public String getSQL(Version currentVersion);
	public R createRow();
	
}
