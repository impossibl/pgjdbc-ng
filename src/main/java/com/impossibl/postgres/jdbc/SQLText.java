package com.impossibl.postgres.jdbc;

import java.sql.SQLException;
import java.text.ParseException;
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
	
	public SQLText(String sqlText) throws ParseException {
		root = parse(sqlText);
	}
	
	public int getStatementCount() {
		return root.getNodeCount();
	}
	
	public StatementNode getLastStatement() {
		return (StatementNode) root.get(root.getNodeCount()-1);
	}
	
	public void process(Processor processor, boolean recurse) throws SQLException {
		root.process(processor, recurse);
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
					"(\\s+)|" +																					/* Whitespace */
					"(\\$\\w*\\$)",																			/* Dollar quote */
					Pattern.MULTILINE);
	
	public static MultiStatementNode parse(String sql) throws ParseException {
		
		Stack<CompositeNode> parents = new Stack<CompositeNode>();
		
		parents.push(new MultiStatementNode(0));
		parents.push(new StatementNode(0));

		Matcher matcher = LEXER.matcher(sql);

		int paramId = 1;
		int startIdx = 0;
		
		try {

			while(matcher.find(startIdx)) {
				
				//Add the unmatched region as grammar...
				if(startIdx != matcher.start()) {
					String txt = sql.substring(startIdx, matcher.start()).trim();
					parents.peek().add(new GrammarPiece(txt, matcher.start()));
				}
	
				startIdx = matcher.end();

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
					
					if(parents.size() == 2) {
						
						CompositeNode comp = parents.pop();
						comp.setEndPos(matcher.end());
						parents.peek().add(comp);					
						parents.push(new StatementNode(matcher.start()));
					}
					else {
						
						parents.peek().add(new GrammarPiece(val, matcher.start()));
					}
					
				}
				else if((val = matcher.group(6)) != null) {
					
					if(val.equals("{")) {
						parents.push(new EscapeNode(matcher.start()));
					}
					else {
						
						if(parents.peek() instanceof EscapeNode) {
							
							EscapeNode tmp = (EscapeNode) parents.pop();
							tmp.setEndPos(matcher.end());
							parents.peek().add(tmp);
						}
						else {
							
							throw new ParseException("Mismatched curly brace", matcher.start());
						}
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

						if(parents.peek() instanceof ParenGroupNode) {
							
							ParenGroupNode tmp = (ParenGroupNode) parents.pop();
							tmp.setEndPos(matcher.end());
							parents.peek().add(tmp);
						}
						else {
							
							throw new ParseException("Mismmatched parenthesis", matcher.start());
						}
					}
				}
				else if((val = matcher.group(10)) != null) {
	
					parents.peek().add(new GrammarPiece(",", matcher.start()));
				}
				else if((val = matcher.group(11)) != null) {
	
					parents.peek().add(new WhitespacePiece(val, matcher.start()));
				}
				else if((val = matcher.group(12)) != null) {

					//Find the end of the $$ quoted block
					int pos = sql.indexOf(val, matcher.end());
					
					//Is this part of an identifier?
					boolean ident = parents.peek().getLastNode() instanceof UnquotedIdentifierPiece;
					
					//For $$ quotes to be valid they
					//	a) need to be closed
					//	b) must not be adjacent to an identifier
					
					if(!ident && pos != -1) {

						String quotedText = sql.substring(matcher.end(), pos);
						
						parents.peek().add(new StringLiteralPiece(quotedText, val, matcher.start()));
						
						startIdx = pos + val.length();
						
					}
					else {
					
						//No end found... treat it as grammar
						parents.peek().add(new GrammarPiece(val, matcher.start()));
					}
					
				}
				
			}
			
			//Add last grammar node
			if(startIdx != sql.length()) {
				parents.peek().add(new GrammarPiece(sql.substring(startIdx), startIdx));
			}
			
			//Auto close last statement
			if(parents.peek() instanceof StatementNode) {
				
				StatementNode stmt = (StatementNode) parents.peek();
				
				stmt.trim();
				
				if(stmt.getNodeCount() > 0) {
					CompositeNode tmp = parents.pop();
					tmp.setEndPos(startIdx);
					parents.peek().add(tmp);
				}
			}
			
			return (MultiStatementNode)parents.get(0);
			
		}
		catch(ParseException e) {
			throw e;
		}
		catch(Exception e) {
			
			//Grab about 10 characters to report context of error
			String errorTxt = sql.substring(startIdx, Math.min(sql.length(), startIdx+10));
			
			throw new ParseException("Error near: " + errorTxt, startIdx);
		}
		
	}

}
