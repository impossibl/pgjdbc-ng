package com.impossibl.postgres.system.tables;

import com.impossibl.postgres.system.UnsupportedServerVersion;
import com.impossibl.postgres.system.Version;


public class Tables {
	
	/**
	 * Helper function for matching a SQL query with a version.
	 * 
	 * @param sqlData An array of [Version, String] pairs to select from
	 * @param currentVersion The requested version of SQL to retrieve
	 * @return SQL text for the requested version
	 * @throws UnsupportedServerVersion if no match can be found
	 * @throws IllegalStateException if the sqlData pairs are ill formed
	 */
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
