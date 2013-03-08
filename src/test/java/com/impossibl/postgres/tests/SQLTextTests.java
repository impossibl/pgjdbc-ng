package com.impossibl.postgres.tests;

import static com.impossibl.postgres.jdbc.PSQLTextUtils.getProtocolSQLText;

import org.junit.Test;


public class SQLTextTests {
	
	@Test
	public void testPostgreSQLText() {
		
		String sql =
				"select \"somthing\" -- This is a SQL comment ?WTF?\n" +
				"	from\n" +
				"		test\n" +
				"	where\n" +
				"		'a string with a ?' =  ?";
		
		sql = getProtocolSQLText(sql);

		sql =
				"insert into \"somthing\" -- This is a SQL comment ?WTF?\n" +
				"	(a, \"b\", \"c\", \"d\")\n" +
				"	values /* a nested\n" +
				" /* comment with  */ a ? */" +
				"	(?,'a string with a ?', \"another ?\", ?, ?)";
		
		sql = getProtocolSQLText(sql);
		
	}

}
