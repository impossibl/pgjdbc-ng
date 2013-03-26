package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.SQLTextEscapeFunctions.getEscapeMethod;
import static com.impossibl.postgres.jdbc.SQLTextEscapeFunctions.invokeEscape;

import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.impossibl.postgres.jdbc.SQLTextTree.CompositeNode;
import com.impossibl.postgres.jdbc.SQLTextTree.EscapeNode;
import com.impossibl.postgres.jdbc.SQLTextTree.IdentifierPiece;
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
			public Node process(Node node) throws SQLException {
				
				if(node instanceof EscapeNode == false) {
					return node;
				}

				return processEscape((EscapeNode) node, context);
			}
			
		});
		
	}

	private static PieceNode processEscape(EscapeNode escape, Context context) throws SQLException {
		
		escape.removeAll(WhitespacePiece.class);
		
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

		checkSize(escape, 3);
		
		UnquotedIdentifierPiece name = getNode(escape, 1, UnquotedIdentifierPiece.class);
		List<Node> args = split(getNode(escape, 2, ParenGroupNode.class), ",");
		
		Method method = getEscapeMethod(name.toString());
		if(method == null) {
			throw new SQLException("Escape function not supported (" + escape.getStartPos() + "): " + name, "Syntax Error");
		}
		
		return invokeEscape(method, name.toString(), args);
	}

	private static String processDateEscape(EscapeNode escape, Context context) throws SQLException {
		
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
		
		checkMinSize(escape, 8);
		checkLiteralNode(escape, 3, "OUTER");
		checkLiteralNode(escape, 4, "JOIN");
		checkLiteralNode(escape, 6, "ON");		
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(getNode(escape, 1, IdentifierPiece.class)).append(" ")
			.append(getNode(escape, 2, UnquotedIdentifierPiece.class))
			.append(" OUTER JOIN ")
			.append(getNode(escape, 5, IdentifierPiece.class))
			.append(" ON (");
		
		ListIterator<Node> argsIter = escape.subList(7).listIterator();
		while(argsIter.hasNext()) {
			
			argsIter.next().build(sb);
			
			if(argsIter.hasNext()) {
				sb.append(' ');
			}
		}
		
		
		sb.append(")");
		
		return sb.toString();
	}

	private static String processCallAssignEscape(EscapeNode escape) throws SQLException {

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

		checkSize(escape, 2, 3);
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("(SELECT * FROM ")
			.append(getNode(escape, 1, UnquotedIdentifierPiece.class));
		
		getNode(escape, 2, ParenGroupNode.class).build(sb);
		
		sb.append(')');
		
		return sb.toString();
	}

	private static String processLimitEscape(EscapeNode escape) throws SQLException {

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
		
		if(escape.getNodeCount() != 2) {
			throw new SQLException("Invalid like escape (" + escape.getStartPos() + ")");
		}

		StringBuilder sb = new StringBuilder();
		
		getNode(escape, 1, Node.class).build(sb);
		
		return sb.toString();		
	}

	private static void checkMinSize(CompositeNode comp, int size) throws SQLException {

		if(comp.getNodeCount() < size) {
			throw new SQLException("Invalid escape syntax (" + comp.getStartPos() + ")", "Syntax Error");
		}
		
	}
	
	private static void checkSize(CompositeNode comp, int... sizes) throws SQLException {

		for(int size : sizes) {
			if(comp.getNodeCount() == size) {
				return;
			}
		}
		
		throw new SQLException("Invalid escape syntax (" + comp.getStartPos() + ")", "Syntax Error");
	}
	
	private static void checkLiteralNode(CompositeNode comp, int index, String text) throws SQLException {
		
		if(getNode(comp, index, Node.class).toString().toUpperCase().equals(text.toUpperCase()) == false) {
			throw new SQLException("Invalid escape (" + comp.getStartPos() + ")", "Syntax Error");
		}
		
	}
	
	private static <T extends Node> T getNode(CompositeNode comp, int idx, Class<T> nodeType) throws SQLException {
		
		Node node = comp.get(idx);
		if(nodeType.isInstance(node) == false)
			throw new SQLException("invalid escape (" + comp.getStartPos() + ")", "Syntax Error");
		
		return nodeType.cast(node);
	}

	private static List<Node> split(CompositeNode group, String text) {
		
		if(group.getNodeCount() == 0) {
			return Collections.emptyList();
		}
		
		CompositeNode current = new CompositeNode(group.getStartPos());

		List<Node> comps = new ArrayList<>();
		comps.add(current);
		
		Iterator<Node> nodeIter = group.iterator();
		while(nodeIter.hasNext()) {
			
			Node node = nodeIter.next();
			
			if(node instanceof PieceNode && ((PieceNode) node).getText().equals(text)) {
				
				current = new CompositeNode(node.getEndPos());
				comps.add(current);
			}
			else {
				
				current.add(node);
			}
			
		}
		
		return comps;
	}

}
