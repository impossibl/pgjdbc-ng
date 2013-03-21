package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.SQLTextEscapeFunctions.getEscapeMethod;
import static com.impossibl.postgres.jdbc.SQLTextEscapeFunctions.invokeEscape;

import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.impossibl.postgres.jdbc.SQLTextTree.CompositeNode;
import com.impossibl.postgres.jdbc.SQLTextTree.EscapeNode;
import com.impossibl.postgres.jdbc.SQLTextTree.GrammarPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.IdentifierPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.Node;
import com.impossibl.postgres.jdbc.SQLTextTree.PieceNode;
import com.impossibl.postgres.jdbc.SQLTextTree.StringLiteralPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.UnquotedIdentifierPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.WhitespacePiece;

public class SQLTextEscapes {
	
	static void processEscapes(SQLText text) throws SQLException {
		
		Map<Node,Node> replacements = new HashMap<>();
		
		for(EscapeNode escape : text.gather(SQLTextTree.EscapeNode.class)) {
			
			SQLTextTree.PieceNode replacement = processEscape(escape);
			
			replacements.put(escape, replacement);
		}
		
		text.replace(replacements);
	}

	private static PieceNode processEscape(EscapeNode escape) throws SQLException {
		
		escape.removeAll(WhitespacePiece.class);
		
		UnquotedIdentifierPiece type = getNode(escape, 0, UnquotedIdentifierPiece.class);
		
		String result = null;
		
		switch(type.toString().toLowerCase()) {
		case "fn":
			result = processFunctionEscape(escape);
			break;
			
		case "d":
			result = processDateEscape(escape);
			break;
			
		case "t":
			result = processTimeEscape(escape);
			break;
			
		case "ts":
			result = processTimestampEscape(escape);
			break;
			
		case "oj":
			result = processOuterJoinEscape(escape);
			break;
			
		case "call":
		case "?":
			result = processCallEscape(escape);
			break;
			
		case "limit":
			result = processLimitEscape(escape);
			break;
			
		case "escape":
			result = processLikeEscape(escape);
			break;
		}
		
		if(result == null)
			throw new SQLException("Invalid escape (" + escape.getStartPos() + ")", "Syntax Error");
		
		return new GrammarPiece(result, escape.getStartPos());
	}
	
	private static String processFunctionEscape(EscapeNode escape) throws SQLException {

		checkMinSize(escape, 4);
		checkLiteralNode(escape, 2, "(");
		checkLiteralNode(escape, escape.getNodeCount()-1, ")");
		
		escape.removeAll(GrammarPiece.class);
		
		checkMinSize(escape, 2);
		
		UnquotedIdentifierPiece name = getNode(escape, 1, UnquotedIdentifierPiece.class);
		List<Node> args = escape.subList(2);
		
		Method method = getEscapeMethod(name.toString());
		if(method == null) {
			throw new SQLException("Escape function not supported (" + escape.getStartPos() + "): " + name, "Syntax Error");
		}
		
		return invokeEscape(method, name.toString(), args);
	}

	private static String processDateEscape(EscapeNode escape) throws SQLException {
		
		checkMinSize(escape, 2);
		
		StringLiteralPiece dateLit = getNode(escape, 1, StringLiteralPiece.class);
		
		Date date = Date.valueOf(dateLit.toString());

		return "'" + date.toString() + "'";
	}

	private static String processTimeEscape(EscapeNode escape) throws SQLException {

		checkMinSize(escape, 2);
		
		StringLiteralPiece timeLit = getNode(escape, 1, StringLiteralPiece.class);
		
		Time time = Time.valueOf(timeLit.toString());
		
		return "'" + time.toString() + "'";
	}

	private static String processTimestampEscape(EscapeNode escape) throws SQLException {

		checkMinSize(escape, 2);
		
		StringLiteralPiece tsLit = getNode(escape, 1, StringLiteralPiece.class);
		
		Timestamp timestamp = Timestamp.valueOf(tsLit.toString());
		
		return "'" + timestamp.toString() + "'";
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
		
		Joiner.on(' ').appendTo(sb, escape.subList(7));
		
		sb.append(")");
		
		return sb.toString();
	}

	private static String processCallEscape(EscapeNode escape) throws SQLException {

		escape.removeAll(GrammarPiece.class);
		
		checkMinSize(escape, 2);
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("(SELECT * FROM ")
			.append(getNode(escape, 1, UnquotedIdentifierPiece.class))
			.append("(");

		Joiner.on(",").appendTo(sb, escape.subList(2));
		
		sb.append("))");
		
		return sb.toString();
	}

	private static String processLimitEscape(EscapeNode escape) {
		// TODO Auto-generated method stub
		return "";
	}

	private static String processLikeEscape(EscapeNode escape) {
		// TODO Auto-generated method stub
		return "";		
	}

	private static void checkMinSize(CompositeNode comp, int size) throws SQLException {

		if(comp.getNodeCount() < size) {
			throw new SQLException("Invalid escape syntax (" + comp.getStartPos() + ")", "Syntax Error");
		}
		
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

}
