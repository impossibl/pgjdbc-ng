package com.impossibl.postgres.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impossibl.postgres.utils.Timer;

public class PerformanceTest {
	
	static Connection conn;
	

	@Before
	public void setUp() throws Exception {
		conn = TestUtil.openDB();
	}

	@After
	public void tearDown() throws Exception {
		TestUtil.closeDB(conn);
	}
	
	@Test
	public void testLargeResultSet() throws Exception {
		
		Timer timer = new Timer();
		
		for(int c=0; c < 100; ++c) {
			
			try(Statement st = conn.createStatement()) {
			
				try(ResultSet rs = st.executeQuery("SELECT id, md5(random()::text) AS descr FROM (SELECT * FROM generate_series(1,100000) AS id) AS x;")) {
					
					while(rs.next()) {
						rs.getString(1);
					}
					
				}
				
			}
		
			System.out.println("Query Time:" + timer.getLapSeconds());
		}
		
	}
	
}
