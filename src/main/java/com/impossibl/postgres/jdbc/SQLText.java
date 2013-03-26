package com.impossibl.postgres.jdbc;

import java.sql.SQLException;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.impossibl.postgres.jdbc.SQLTextTree.CommentPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.CompositeNode;
import com.impossibl.postgres.jdbc.SQLTextTree.EscapeNode;
import com.impossibl.postgres.jdbc.SQLTextTree.GrammarPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.MultiStatementNode;
import com.impossibl.postgres.jdbc.SQLTextTree.NumericLiteralPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.ParameterPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.ParenGroupNode;
import com.impossibl.postgres.jdbc.SQLTextTree.Processor;
import com.impossibl.postgres.jdbc.SQLTextTree.QuotedIdentifierPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.StatementNode;
import com.impossibl.postgres.jdbc.SQLTextTree.StringLiteralPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.UnquotedIdentifierPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.WhitespacePiece;

public class SQLText {
	
	private MultiStatementNode root;	
	
	public SQLText(String sqlText) {
		root = parse(sqlText);
	}
	
	public int getStatementCount() {
		return root.getNodeCount();
	}
	
	public StatementNode getLastStatement() {
		return (StatementNode) root.get(root.getNodeCount()-1);
	}
	
	public void process(Processor processor) throws SQLException {
		root.process(processor);
	}
	
	@Override
	public String toString() {
		return root.toString();
	}
	
	/*
	 * Lexical pattern for the parser that finds these things:
	 *  > SQL identifier
	 * 	> SQL quoted identifier (ignoring escaped double quotes)
	 * 	> Single quoted strings (ignoring escaped single quotes)
	 * 	> SQL comments... from "--" to end of line
	 *  > C-Style comments (including nested sections)
	 *  > ? Parameter placeholders
	 *  > ; Statement breaks
	 */
	private static final Pattern LEXER = Pattern
			.compile(
					"(?:\"((?:[^\"\"]|\\\\.)*)\")|" +										/* Quoted identifier */
					"(?:'((?:[^\'\']|\\\\.)*)')|" +											/* String literal */
					"((?:\\-\\-.*$)|(?:/\\*(?:(?:.|\\n)*)\\*/))|" +			/* Comments */
					"(\\?)|" +																					/* Parameter marker */
					"(;)|" +																						/* Statement break */
					"(\\{|\\})|" +																			/* Escape open/close */
					"([a-zA-Z_][\\w_]*)|" +															/* Unquoted identifier */
					"((?:[+-]?(?:\\d+)?(?:\\.\\d+(?:[eE][+-]?\\d+)?))|(?:[+-]?\\d+))|" + /* Numeric literal */
					"(\\(|\\))|" +																			/* Parens (grouping) */
					"(,)|" +																						/* Comma (breaking) */
					"(\\s+)",																						/* Whitespace */
					Pattern.MULTILINE);

	public static MultiStatementNode parse(String sql) {
		
		Stack<CompositeNode> parents = new Stack<CompositeNode>();
		
		parents.push(new MultiStatementNode(0));
		parents.push(new StatementNode(0));

		Matcher matcher = LEXER.matcher(sql);

		int paramId = 1;
		int startIdx = 0;

		while(matcher.find()) {
			
			//Add the unmatched region as grammar...
			if(startIdx != matcher.start()) {
				String txt = sql.substring(startIdx, matcher.start()).trim();
				parents.peek().add(new GrammarPiece(txt, matcher.start()));
			}

			//Add whatever we matched...
			String val;
			if((val = matcher.group(1)) != null) {
				
				parents.peek().add(new QuotedIdentifierPiece(val, matcher.start()));
			}
			else if((val = matcher.group(2)) != null) {
				
				parents.peek().add(new StringLiteralPiece(val, matcher.start()));
			}
			else if((val = matcher.group(3)) != null) {
				
				parents.peek().add(new CommentPiece(val, matcher.start()));
			}
			else if((val = matcher.group(4)) != null) {
				
				parents.peek().add(new ParameterPiece(paramId++, matcher.start()));
			}
			else if((val = matcher.group(5)) != null) {
				
				//Pop & add everything until the top node
				while(parents.size() > 1) {
					
					CompositeNode comp = parents.pop();
					comp.setEndPos(matcher.end());
					parents.peek().add(comp);
				}
				
				parents.push(new StatementNode(matcher.start()));
			}
			else if((val = matcher.group(6)) != null) {
				
				if(val.equals("{")) {
					parents.push(new EscapeNode(matcher.start()));
				}
				else {
					EscapeNode tmp = (EscapeNode) parents.pop();
					tmp.setEndPos(matcher.end());
					parents.peek().add(tmp);
				}
			}
			else if((val = matcher.group(7)) != null) {

				parents.peek().add(new UnquotedIdentifierPiece(val, matcher.start()));
			}
			else if((val = matcher.group(8)) != null) {

				parents.peek().add(new NumericLiteralPiece(val, matcher.start()));
			}
			else if((val = matcher.group(9)) != null) {

				if(val.equals("(")) {
					parents.push(new ParenGroupNode(matcher.start()));
				}
				else {
					ParenGroupNode tmp = (ParenGroupNode) parents.pop();
					tmp.setEndPos(matcher.end());
					parents.peek().add(tmp);
				}
			}
			else if((val = matcher.group(10)) != null) {

				parents.peek().add(new GrammarPiece(",", matcher.start()));
			}
			else if((val = matcher.group(11)) != null) {

				parents.peek().add(new WhitespacePiece(val, matcher.start()));
			}
			
			startIdx = matcher.end();			
		}
		
		//Add last grammar node
		if(startIdx != sql.length()) {
			parents.peek().add(new GrammarPiece(sql.substring(startIdx), startIdx));
		}
		
		//Auto close last statement
		if(parents.peek() instanceof StatementNode) {
			CompositeNode tmp = parents.pop();
			tmp.setEndPos(startIdx);
			parents.peek().add(tmp);
		}
		
		return (MultiStatementNode)parents.get(0);
	}

}
