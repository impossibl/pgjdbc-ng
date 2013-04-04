package com.impossibl.postgres.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;



public class SQLTextTree {
	
	public interface Processor {
		
		public Node process(Node node, boolean recurse) throws SQLException;
		
	}

	public static abstract class Node {

		private int startPos;
		private int endPos;

		public Node(int startPos, int endPos) {
			this.startPos = startPos;
			this.endPos = endPos;
		}

		public int getStartPos() {
			return startPos;
		}

		public void setStartPos(int start) {
			this.startPos = start;
		}

		public int getEndPos() {
			return endPos;
		}

		public void setEndPos(int end) {
			this.endPos = end;
		}
		
		abstract void build(StringBuilder builder);
		
		public Node process(Processor processor, boolean recurse) throws SQLException {
			return processor.process(this, recurse);
		}

		void removeAll(final Class<? extends Node> nodeType, boolean recurse) {
			try {
				process(new Processor() {

					@Override
					public Node process(Node node, boolean recurse) throws SQLException {
						if(nodeType.isInstance(node))
							return null;
						return node;
					}
				}, recurse);
			}
			catch(SQLException e) {
			}
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			build(sb);
			return sb.toString();
		}

	}

	public static class CompositeNode extends Node {

		protected List<Node> nodes = new ArrayList<>();

		public CompositeNode(int startPos) {
			super(startPos, -1);
		}

		void build(StringBuilder builder) {

			for(Node node : nodes) {
				node.build(builder);
			}

		}
		
		int getNodeCount() {
			return nodes.size();
		}

		Node get(int idx) {
			return nodes.get(idx);
		}

		void set(int idx, Node node) {
			nodes.set(idx, node);
		}
		
		Iterator<Node> iterator() {
			return nodes.iterator();
		}
		
		List<Node> subList(int fromIndex) {
			return subList(fromIndex, nodes.size());
		}

		List<Node> subList(int fromIndex, int toIndex) {
			return nodes.subList(fromIndex, toIndex);
		}
		
		public Node process(Processor processor, boolean recurse) throws SQLException {
			
			//Process each child node...
			ListIterator<Node> nodeIter = nodes.listIterator();
			while(nodeIter.hasNext()) {
				Node res = nodeIter.next().process(processor, recurse);
				if(res != null) {
					nodeIter.set(res);
				}
				else {
					nodeIter.remove();
				}
			}
			
			return processor.process(this, recurse);
		}

		void add(Node node) {

			this.nodes.add(node);
		}

		public boolean containsAll(Class<? extends Node> cls) {
			
			for(Node node : nodes) {
				if(cls.isInstance(node) == false)
					return false;
			}
			
			return true;
		}
		
		public void trim() {
			
			if(nodes.isEmpty())
				return;
			
			//Prune starting and ending whitespace
			if(nodes.get(0) instanceof WhitespacePiece) {
				nodes.remove(0);
			}
			
			if(nodes.isEmpty()) {
				return;
			}
			
			if(nodes.get(nodes.size()-1) instanceof WhitespacePiece) {
				nodes.remove(nodes.size()-1);
			}
			
		}

		public Node getFirstNode() {
			if(nodes.isEmpty())
				return null;
			return nodes.get(0);
		}

		public Node getLastNode() {
			if(nodes.isEmpty())
				return null;
			return nodes.get(nodes.size()-1);
		}

	}

	public static class MultiStatementNode extends CompositeNode {

		public MultiStatementNode(int startPos) {
			super(startPos);
		}

		@Override
		void build(StringBuilder builder) {

			Iterator<Node> nodeIter = iterator();
			while(nodeIter.hasNext()) {
				nodeIter.next().build(builder);
				if(nodeIter.hasNext())
					builder.append(';');
			}
		}

	}

	public static class StatementNode extends CompositeNode {

		public StatementNode(int startPos) {
			super(startPos);
		}

	}

	public static class EscapeNode extends CompositeNode {

		public EscapeNode(int startPos) {
			super(startPos);
		}

		@Override
		void build(StringBuilder builder) {

			builder.append('{');

			super.build(builder);

			builder.append('}');
		}

	}

	public static class ParenGroupNode extends CompositeNode {

		public ParenGroupNode(int startPos) {
			super(startPos);
		}
		
		@Override
		void build(StringBuilder builder) {

			builder.append('(');

			super.build(builder);

			builder.append(')');
		}

	}

	public static class PieceNode extends Node {

		private String text;

		public PieceNode(String val, int startPos) {
			super(startPos, startPos + val.length());
			this.text = val;
		}

		public String getText() {
			return text;
		}

		public void setText(String text) {
			this.text = text;
		}

		void build(StringBuilder builder) {
			builder.append(text);
		}

		@Override
		public String toString() {
			return text;
		}

	}
	
	public static class GrammarPiece extends PieceNode {

		GrammarPiece(String val, int startPos) {
			super(val, startPos);
		}

	}

	public static class ParameterPiece extends PieceNode {

		int idx;

		ParameterPiece(int idx, int pos) {
			super("$"+idx, pos);
		}

	}
	
	public static class IdentifierPiece extends PieceNode {

		public IdentifierPiece(String val, int startPos) {
			super(val, startPos);
		}
		
	}

	public static class UnquotedIdentifierPiece extends IdentifierPiece {

		UnquotedIdentifierPiece(String val, int startPos) {
			super(val, startPos);
		}

	}

	public static class QuotedIdentifierPiece extends IdentifierPiece {

		QuotedIdentifierPiece(String val, int startPos) {
			super(val, startPos);
		}

		@Override
		void build(StringBuilder builder) {

			builder.append('"');

			super.build(builder);

			builder.append('"');
		}

	}
	
	public static class LiteralPiece extends PieceNode {

		public LiteralPiece(String val, int startPos) {
			super(val, startPos);
		}
		
	}

	public static class StringLiteralPiece extends LiteralPiece {

		String delimeter;
		
		StringLiteralPiece(String val, int startPos) {
			super(val, startPos);
			this.delimeter = "'";
		}

		StringLiteralPiece(String val, String delimeter, int startPos) {
			super(val, startPos);
			this.delimeter = delimeter;
		}

		@Override
		void build(StringBuilder builder) {

			builder.append(delimeter);

			super.build(builder);

			builder.append(delimeter);
		}

	}

	public static class NumericLiteralPiece extends LiteralPiece {

		NumericLiteralPiece(String val, int startPos) {
			super(val, startPos);
		}

	}

	public static class ReplacementPiece extends PieceNode {

		ReplacementPiece(String val, int startPos) {
			super(val, startPos);
		}

	}

	public static class CommentPiece extends PieceNode {

		CommentPiece(String val, int startPos) {
			super(val, startPos);
		}

	}

	public static class WhitespacePiece extends PieceNode {

		WhitespacePiece(String val, int startPos) {
			super(val, startPos);
		}

	}

}
