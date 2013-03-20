package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.SQLTextUtils.getProtocolSQLText;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.impossibl.postgres.jdbc.SQLTextTree.EscapeNode;
import com.impossibl.postgres.jdbc.SQLTextTree.Node;
import com.impossibl.postgres.jdbc.SQLTextTree.StringLiteralPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.WhitespacePiece;


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
				"	(?,'a string with a ?', \"another  \"\" ?\", {fn some('{fn '' some()}', {fn thing(?)}}, ?)",
			
				//Output
				"insert into \"somthing\" -- This is a SQL comment ?WTF?\n" +
				"	(a, \"b\", \"c\", \"d\")\n" +
				"	values /* a nested\n" +
				" /* comment with  */ a ? */" +
				"	($1,'a string with a ?', \"another \"\" ?\", {fn some('{fn '' some()}', {fn thing($2)}}, $3), $4)",
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
	
	@Test
	public void testParse() {
		
		SQLText text = new SQLText(sqlTransformTests[2][0]);
		
		List<EscapeNode> escapes = text.gather(SQLTextTree.EscapeNode.class);
		
		escapes.get(0).removeAll(WhitespacePiece.class);
		
		Map<Node,Node> map = new HashMap<Node,Node>();
		map.put(escapes.get(0), new StringLiteralPiece("#### REPLACED ####"));
		
		text.replace(map);
				
		System.out.print(escapes);
	}

}
