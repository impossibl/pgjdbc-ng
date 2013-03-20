package com.impossibl.postgres.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.impossibl.postgres.jdbc.SQLTextTree.CommentPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.CompositeNode;
import com.impossibl.postgres.jdbc.SQLTextTree.EscapeNode;
import com.impossibl.postgres.jdbc.SQLTextTree.GrammarPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.MultiStatementNode;
import com.impossibl.postgres.jdbc.SQLTextTree.Node;
import com.impossibl.postgres.jdbc.SQLTextTree.ParameterPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.QuotedIdentifierPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.StatementNode;
import com.impossibl.postgres.jdbc.SQLTextTree.StringLiteralPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.UnquotedIdentifierPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.WhitespacePiece;

public class SQLText {
	
	Node root;	
	
	public SQLText(String sqlText) {
		root = parse(sqlText);
	}
	
	public <N extends Node> List<N> gather(Class<N> nodeType) {
		List<N> nodes = new ArrayList<>();
		root.gather(nodeType, nodes);
		return nodes;
	}
	
	public void replace(Map<Node, Node> nodes) {
		root.replace(nodes);
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
			.compile("(\"(?:[^\"\"]|\\\\.)*\")|('(?:[^\'\']|\\\\.)*')|((?:\\-\\-.*$)|(?:/\\*(?:(?:.|\\n)*)\\*/))|(\\?)|(;)|(\\{|\\})|([a-zA-Z_][\\w_]*)|(\\s+)", Pattern.MULTILINE);

	public static Node parse(String sql) {
		
		Stack<CompositeNode> parents = new Stack<CompositeNode>();
		
		parents.push(new MultiStatementNode());
		parents.push(new StatementNode());

		Matcher matcher = LEXER.matcher(sql);

		int paramId = 1;
		int startIdx = 0;

		while (matcher.find()) {
			
			//Add the unmatched region as grammar...
			if(startIdx != matcher.start()) {
				String txt = sql.substring(startIdx, matcher.start()).trim();
				parents.peek().add(new GrammarPiece(txt));
			}
			startIdx = matcher.end();
			
			//Add whatever we matched...
			String val;
			if((val = matcher.group(1)) != null) {
				parents.peek().add(new QuotedIdentifierPiece(val));
			}
			else if((val = matcher.group(2)) != null) {
				parents.peek().add(new StringLiteralPiece(val));
			}
			else if((val = matcher.group(3)) != null) {
				parents.peek().add(new CommentPiece(val));
			}
			else if((val = matcher.group(4)) != null) {
				parents.peek().add(new ParameterPiece(paramId++));
			}
			else if((val = matcher.group(5)) != null) {
				CompositeNode comp = parents.pop();
				parents.peek().add(comp);
				parents.push(new StatementNode());
			}
			else if((val = matcher.group(6)) != null) {
				if(val.equals("{")) {
					parents.push(new EscapeNode());
				}
				else {
					CompositeNode tmp = parents.pop();
					parents.peek().add(tmp);
				}
			}
			else if((val = matcher.group(7)) != null) {
				parents.peek().add(new UnquotedIdentifierPiece(val));
			}
			else if((val = matcher.group(8)) != null) {
				parents.peek().add(new WhitespacePiece(val));
			}
			
		}
		
		//Add last grammar node
		if(startIdx != sql.length()) {
			parents.peek().add(new GrammarPiece(sql.substring(startIdx)));
		}
		
		//Aut close last staement
		if(parents.peek() instanceof StatementNode) {
			CompositeNode tmp = parents.pop();
			parents.peek().add(tmp);
		}
		
		return parents.get(0);
	}

}
