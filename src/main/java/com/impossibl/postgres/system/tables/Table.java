package com.impossibl.postgres.system.tables;


import java.sql.ResultSet;
import java.sql.SQLException;

import com.impossibl.postgres.system.Version;

public interface Table<R> {
	
	public String getSQL(Version currentVersion);
	public R createRow(ResultSet rs) throws SQLException;
	
}
