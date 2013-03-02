package com.impossibl.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;

import com.impossibl.postgres.system.Version;
import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgProc;
import com.impossibl.postgres.system.tables.PgType;
import com.impossibl.postgres.system.tables.Tables;
import com.impossibl.postgres.types.Registry;

public class Postgres {
	
	private static Logger logger = Logger.getLogger(Postgres.class.getName());
	
	
	public static void init(Connection conn, Version version) {
		
		long start = System.currentTimeMillis(), mid;
		
		try {

			List<PgType.Row> pgTypeData = Tables.load(conn, PgType.INSTANCE, version);
			List<PgAttribute.Row> pgAttrData = Tables.load(conn, PgAttribute.INSTANCE, version);
			List<PgProc.Row> pgProcData = Tables.load(conn, PgProc.INSTANCE, version);
			
			logger.info("load took " + ((mid = System.currentTimeMillis()) - start) + " ms");
			
			Registry.update(pgTypeData, pgAttrData, pgProcData);
			
			logger.info("init took " + (System.currentTimeMillis() - mid) + " ms");
			
			
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		
		
	}

}
