package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.SQLTextEscapeFunctions.getEscapeMethod;
import static com.impossibl.postgres.jdbc.SQLTextEscapeFunctions.invokeEscape;

import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.impossibl.postgres.jdbc.SQLTextTree.CompositeNode;
import com.impossibl.postgres.jdbc.SQLTextTree.EscapeNode;
import com.impossibl.postgres.jdbc.SQLTextTree.Node;
import com.impossibl.postgres.jdbc.SQLTextTree.ParenGroupNode;
import com.impossibl.postgres.jdbc.SQLTextTree.PieceNode;
import com.impossibl.postgres.jdbc.SQLTextTree.Processor;
import com.impossibl.postgres.jdbc.SQLTextTree.ReplacementPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.StringLiteralPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.UnquotedIdentifierPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.WhitespacePiece;
import com.impossibl.postgres.system.Context;

public class SQLTextEscapes {
	
	static void processEscapes(SQLText text, final Context context) throws SQLException {

		text.process(new Processor() {

			@Override
			public Node process(Node node, boolean recurse) throws SQLException {
				
				if(node instanceof EscapeNode == false) {
					return node;
				}

				return processEscape((EscapeNode) node, context);
			}
			
		}, true);
		
	}

	private static PieceNode processEscape(EscapeNode escape, Context context) throws SQLException {
		
		UnquotedIdentifierPiece type = getNode(escape, 0, UnquotedIdentifierPiece.class);
		
		String result = null;
		
		switch(type.toString().toLowerCase()) {
		case "fn":
			result = processFunctionEscape(escape);
			break;
			
		case "d":
			result = processDateEscape(escape, context);
			break;
			
		case "t":
			result = processTimeEscape(escape, context);
			break;
			
		case "ts":
			result = processTimestampEscape(escape, context);
			break;
			
		case "oj":
			result = processOuterJoinEscape(escape);
			break;
			
		case "call":
			result = processCallEscape(escape);
			break;
			
		case "?":
			result = processCallAssignEscape(escape);
			break;
			
		case "limit":
			result = processLimitEscape(escape);
			break;
			
		case "escape":
			result = processLikeEscape(escape);
			break;

		default:
			throw new SQLException("Invalid escape (" + escape.getStartPos() + ")", "Syntax Error");
		}
		
		return new ReplacementPiece(result, escape.getStartPos());
	}
	
	private static String processFunctionEscape(EscapeNode escape) throws SQLException {

		escape.removeAll(WhitespacePiece.class, false);

		checkSize(escape, 3);
		
		UnquotedIdentifierPiece name = getNode(escape, 1, UnquotedIdentifierPiece.class);
		List<Node> args = split(getNode(escape, 2, ParenGroupNode.class), false, ",");
		
		Method method = getEscapeMethod(name.toString());
		if(method == null) {
			throw new SQLException("Escape function not supported (" + escape.getStartPos() + "): " + name, "Syntax Error");
		}
		
		return invokeEscape(method, name.toString(), args);
	}

	private static String processDateEscape(EscapeNode escape, Context context) throws SQLException {
		
		escape.removeAll(WhitespacePiece.class, false);

		checkSize(escape, 2);
		
		StringLiteralPiece dateLit = getNode(escape, 1, StringLiteralPiece.class);
		
		Date date;
		try {
			date = Date.valueOf(dateLit.getText());
		}
		catch(Exception e1) {
			throw new SQLException("invalid date format in escape (" + escape.getStartPos() + ")");
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append("DATE '");
		sb.append(date);
		sb.append("'");
		return sb.toString();
	}

	private static String processTimeEscape(EscapeNode escape, Context context) throws SQLException {

		escape.removeAll(WhitespacePiece.class, false);

		checkSize(escape, 2);
		
		StringLiteralPiece timeLit = getNode(escape, 1, StringLiteralPiece.class);
		
		Time time;
		try {
			time = Time.valueOf(timeLit.toString());
		}
		catch(Exception e1) {
			throw new SQLException("invalid time format in escape (" + escape.getStartPos() + ")");
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append("TIME '");
		sb.append(time);
		sb.append("'");
		return sb.toString();
	}

	private static String processTimestampEscape(EscapeNode escape, Context context) throws SQLException {
		
		escape.removeAll(WhitespacePiece.class, false);

		checkSize(escape, 2);
		
		StringLiteralPiece tsLit = getNode(escape, 1, StringLiteralPiece.class);
		
		Timestamp timestamp;
		try {
			timestamp = Timestamp.valueOf(tsLit.toString());
		}
		catch(Exception e) {
			throw new SQLException("invalid timestamp format in escape (" + escape.getStartPos() + ")");
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append("TIMESTAMP '");
		sb.append(timestamp);
		sb.append("'");
		return sb.toString();
	}

	private static String processOuterJoinEscape(EscapeNode escape) throws SQLException {
		
		List<Node> nodes = split(escape, true, "OJ", "LEFT", "RIGHT", "FULL", "OUTER", "JOIN", "ON");
		
		checkSize(nodes, escape.getStartPos(), 8);
		checkLiteralNode(nodes.get(3), "OUTER");
		checkLiteralNode(nodes.get(4), "JOIN");
		checkLiteralNode(nodes.get(6), "ON");		

		StringBuilder sb = new StringBuilder();
		
		nodes.get(1).build(sb);
		sb.append(' ');
		nodes.get(2).build(sb);
		sb.append(" OUTER JOIN ");
		nodes.get(5).build(sb);
		sb.append(" ON ");
		nodes.get(7).build(sb);
		
		return sb.toString();
	}

	private static String processCallAssignEscape(EscapeNode escape) throws SQLException {

		escape.removeAll(WhitespacePiece.class, false);
		
		checkSize(escape, 4, 5);
		
		checkLiteralNode(escape, 1, "=");
		checkLiteralNode(escape, 2, "call");
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("(SELECT * FROM ")
			.append(getNode(escape, 1, UnquotedIdentifierPiece.class));
		
		getNode(escape, 3, ParenGroupNode.class).build(sb);
		
		sb.append(")");
		
		return sb.toString();
	}

	private static String processCallEscape(EscapeNode escape) throws SQLException {

		escape.removeAll(WhitespacePiece.class, false);
		
		checkSize(escape, 2, 3);
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("(SELECT * FROM ")
			.append(getNode(escape, 1, UnquotedIdentifierPiece.class));
		
		getNode(escape, 2, ParenGroupNode.class).build(sb);
		
		sb.append(')');
		
		return sb.toString();
	}

	private static String processLimitEscape(EscapeNode escape) throws SQLException {

		escape.removeAll(WhitespacePiece.class, false);
		
		checkSize(escape, 2, 4);
		
		Node rows = getNode(escape, 1, Node.class);
		
		Node offset = null;
		if(escape.getNodeCount() == 4) {
			checkLiteralNode(escape, 2, "OFFSET");
			offset = getNode(escape, 3, Node.class);;
		}
		
		StringBuilder sb = new StringBuilder();

		sb.append("LIMIT ");
		
		rows.build(sb);
		
		if(offset != null) {
			sb.append(" OFFSET ");
			offset.build(sb);
		}
		
		return sb.toString();
	}

	private static String processLikeEscape(EscapeNode escape) throws SQLException {
		
		escape.removeAll(WhitespacePiece.class, false);
		
		checkSize(escape, 2);

		StringBuilder sb = new StringBuilder();
		
		sb.append("ESCAPE ");
		getNode(escape, 1, Node.class).build(sb);
		
		return sb.toString();		
	}

	private static void checkSize(CompositeNode comp, int... sizes) throws SQLException {

		checkSize(comp.nodes, comp.getStartPos(),  sizes);
	}
	
	private static void checkSize(List<Node> nodes, int startPos, int... sizes) throws SQLException {

		for(int size : sizes) {
			if(nodes.size() == size) {
				return;
			}
		}
		
		throw new SQLException("Invalid escape syntax (" + startPos + ")", "Syntax Error");
	}
	
	private static void checkLiteralNode(CompositeNode comp, int index, String text) throws SQLException {
		
		checkLiteralNode(getNode(comp, index, Node.class), text);		
	}
	
	private static void checkLiteralNode(Node test, String text) throws SQLException {
		
		if(test.toString().toUpperCase().equals(text.toUpperCase()) == false) {
			throw new SQLException("Invalid escape (" + test.getStartPos() + ")", "Syntax Error");
		}
		
	}
	
	private static <T extends Node> T getNode(CompositeNode comp, int idx, Class<T> nodeType) throws SQLException {
		
		Node node = comp.get(idx);
		if(nodeType.isInstance(node) == false)
			throw new SQLException("invalid escape (" + comp.getStartPos() + ")", "Syntax Error");
		
		return nodeType.cast(node);
	}

	private static List<Node> split(CompositeNode comp, boolean includeMatches, String... matches) {
	
		return split(comp.nodes, comp.getStartPos(), includeMatches, matches);
	}
	
	private static List<Node> split(List<Node> nodes, int startPos, boolean includeMatches, String... matches) {
		
		List<String> matchList = Arrays.asList(matches);
		
		if(nodes.size() == 0) {
			return Collections.emptyList();
		}
		
		CompositeNode current = new CompositeNode(startPos);

		List<Node> comps = new ArrayList<>();
		
		Iterator<Node> nodeIter = nodes.iterator();
		while(nodeIter.hasNext()) {
			
			Node node = nodeIter.next();
			
			if(node instanceof PieceNode && matchList.contains(((PieceNode) node).getText().toUpperCase())) {
				
				trim(current);
				
				if(current.getNodeCount() > 0) {
					
					comps.add(current);
					current = new CompositeNode(node.getEndPos());
					
				}
								
				if(includeMatches) {
					comps.add(node);
				}
				
			}
			else {
				
				current.add(node);
			}
			
		}
		
		trim(current);
		
		if(current.getNodeCount() > 0) {
			comps.add(current);
		}
		
		return comps;
	}
	
	private static void trim(CompositeNode comp) {
		
		if(comp.getNodeCount() == 0)
			return;
		
		//Prune starting and ending whitespace
		if(comp.get(0) instanceof WhitespacePiece) {
			comp.nodes.remove(0);
		}
		
		if(comp.nodes.isEmpty()) {
			return;
		}
		
		if(comp.get(comp.nodes.size()-1) instanceof WhitespacePiece) {
			comp.nodes.remove(comp.nodes.size()-1);
		}
		
	}

}
