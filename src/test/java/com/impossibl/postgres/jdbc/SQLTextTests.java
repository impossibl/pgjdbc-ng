/**
 * Copyright (c) 2013, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.impossibl.postgres.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.SQLException;
import java.text.ParseException;

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
		},
		new String[] {
				
				//Input
				"insert into \"somthing\" -- This is a SQL comment ?WTF?\n" +
				"	(a, \"b\", \"c\", \"d\")\n" +
				"	values /* a nested\n" +
				" /* comment with  */ a ? */" +
				"	(?,'a string with a ?', \"another \"\" ?\", {fn concat('{fn '' some()}', {fn char(?)})}, ?)",
			
				//Output
				"insert into \"somthing\" -- This is a SQL comment ?WTF?\n" +
				"	(a, \"b\", \"c\", \"d\")\n" +
				"	values /* a nested\n" +
				" /* comment with  */ a ? */" +
				"	($1,'a string with a ?', \"another \"\" ?\", ('{fn '' some()}'||chr($2)), $3)",
		},
		new String[] {
				"select {fn abs(-10)} as absval, {fn user()}, {fn concat(x,y)} as val from {oj tblA left outer join tblB on x=y}",
				"select abs(-10) as absval, user, (x||y) as val from tblA left OUTER JOIN tblB ON x=y",
		}
	};

	/**
	 * Tests transforming JDBC SQL input into PostgreSQL's wire
	 * protocol format. 
	 * @throws SQLException 
	 * @throws ParseException 
	 * 
	 * @see SQLTextUtils.getProtocolSQLText 
	 */
	@Test
	public void testPostgreSQLText() throws SQLException, ParseException {

		for(String[] test : sqlTransformTests) {
			
			String expected = test[1];
			
			SQLText sqlText = new SQLText(test[0]);
			
			SQLTextEscapes.processEscapes(sqlText, null);
			
			assertThat(sqlText.toString(), is(equalTo(expected)));
		}
	}

}
