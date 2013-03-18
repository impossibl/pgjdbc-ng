package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.PSQLTextUtils.getProtocolSQLText;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;


public class SQLTextTests {

	String[][] sqlTransformTests = new String[][] {
		new String[] {
				
				//Input
				"select \"somthing\" -- This is a SQL comment ?WTF?\n" +
				"	from\n" +
				"		test\n" +
				"	where\n" +
				"		'a string with a ?' =  ?",
				
				//Output
				"select \"somthing\" -- This is a SQL comment ?WTF?\n" +
				"	from\n" +
				"		test\n" +
				"	where\n" +
				"		'a string with a ?' =  $1"
		},			
		new String[] {
	
				//Input
				"insert into \"somthing\" -- This is a SQL comment ?WTF?\n" +
				"	(a, \"b\", \"c\", \"d\")\n" +
				"	values /* a nested\n" +
				" /* comment with  */ a ? */" +
				"	(?,'a string with a ?', \"another ?\", ?, ?)",
			
				//Output
				"insert into \"somthing\" -- This is a SQL comment ?WTF?\n" +
				"	(a, \"b\", \"c\", \"d\")\n" +
				"	values /* a nested\n" +
				" /* comment with  */ a ? */" +
				"	($1,'a string with a ?', \"another ?\", $2, $3)",
		}
	};

	/**
	 * Tests transforming JDBC SQL input into PostgreSQL's wire
	 * protocol format. 
	 * 
	 * @see SQLTextUtils.getProtocolSQLText 
	 */
	@Test
	public void testPostgreSQLText() {

		for(String[] test : sqlTransformTests) {
			
			String expected = test[1];
			String output = getProtocolSQLText(test[0]);
			
			assertThat(output, is(equalTo(expected)));
		}
	}

}
