package com.impossibl.postgres.system.tables;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.impossibl.postgres.system.UnsupportedServerVersion;
import com.impossibl.postgres.system.Version;


public class Tables {
	
	public static <T extends Table<R>, R> List<R> load(Connection conn, T table, Version version) throws SQLException {

		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(table.getSQL(version));
		
			ps.execute();
		
			return Tables.load(table, ps.getResultSet());
		}
		finally {
			if(ps != null) ps.close();
		}
		
	}

	public static <T extends Table<R>, R> List<R> load(T table, ResultSet results) throws SQLException {
		
		List<R> rows = new ArrayList<R>();
		
		while(results.next()) {
			
			rows.add(table.createRow(results));
			
		}
		
		return rows;
	}

	public static String getSQL(Object[] sqlData, Version currentVersion) {

		try {

			for(int c=0; c < sqlData.length; c += 2) {
				
				Version curSqlVersion = (Version) sqlData[c];
				String curSql = (String) sqlData[c+1];
				
				if(curSqlVersion.compatible(currentVersion))
					return curSql;
			}
			
		}
		catch (Exception e) {
			throw new IllegalStateException("Misconfigured system table type ");
		}
		
		throw new UnsupportedServerVersion(currentVersion);
	}

}
