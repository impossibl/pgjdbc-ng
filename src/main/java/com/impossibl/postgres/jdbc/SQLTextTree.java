package com.impossibl.postgres.jdbc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;



public class SQLTextTree {

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

		<T extends Node> void gather(Class<T> nodeType, List<T> nodes) {
			if(nodeType.isInstance(this)) {
				nodes.add(nodeType.cast(this));
			}
		}

		void replace(Map<Node, Node> nodes) {
		}

		void removeAll(Class<? extends Node> nodeType) {
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			build(sb);
			return sb.toString();
		}

	}

	public static abstract class CompositeNode extends Node {

		private List<Node> nodes = new ArrayList<>();

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
		
		List<Node> subList(int fromIndex) {
			return subList(fromIndex, nodes.size());
		}

		List<Node> subList(int fromIndex, int toIndex) {
			return nodes.subList(fromIndex, toIndex);
		}

		<T extends Node> void gather(Class<T> nodeType, List<T> nodes) {
			super.gather(nodeType, nodes);
			for(Node node : this.nodes) {
				node.gather(nodeType, nodes);
			}
		}

		void replace(Map<Node, Node> nodes) {

			ListIterator<Node> nodeIter = this.nodes.listIterator();
			while(nodeIter.hasNext()) {

				Node node = nodeIter.next();
				Node replacement = nodes.get(node);
				if(replacement != null) {
					nodeIter.set(replacement);
				}
				else {
					node.replace(nodes);
				}
			}

		}

		void removeAll(Class<? extends Node> nodeType) {

			Iterator<Node> nodeIter = nodes.iterator();
			while(nodeIter.hasNext()) {
				if(nodeType.isInstance(nodeIter.next())) {
					nodeIter.remove();
				}
			}

		}

		void add(Node node) {

			this.nodes.add(node);
		}

	}

	public static class MultiStatementNode extends CompositeNode {

		public MultiStatementNode(int startPos) {
			super(startPos);
		}

	}

	public static class StatementNode extends CompositeNode {

		public StatementNode(int startPos) {
			super(startPos);
		}

		@Override
		void build(StringBuilder builder) {

			super.build(builder);

			builder.append(';');
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

	}

	public static class StringLiteralPiece extends PieceNode {

		StringLiteralPiece(String val, int startPos) {
			super(val, startPos);
		}

	}

	public static class NumericLiteralPiece extends PieceNode {

		NumericLiteralPiece(String val, int startPos) {
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
