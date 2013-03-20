package com.impossibl.postgres.jdbc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class PSQLTextTree {

	public static abstract class Node {
		
		abstract void build(StringBuilder builder) ;
		
		<T extends Node> void gather(Class<T> nodeType, List<T> nodes) {
			if(nodeType.isInstance(this)) {
				nodes.add(nodeType.cast(this));
			}
		}
		
		void replace(Map<Node,Node> nodes) {
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
		
		List<Node> nodes = new ArrayList<>();
		
		void build(StringBuilder builder) {
	
			for(Node node : nodes) {
				node.build(builder);
			}
			
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
			
			//Offer pieces to the last piece added to give
			//them a chance to combine into a single piece
			if(!nodes.isEmpty()) {
				
				Node last = nodes.get(nodes.size()-1);
				
				if(last instanceof PieceNode) {
					
					if(((PieceNode)last).consume(node))
						return;
				}
			}
			
			this.nodes.add(node);
		}
		
	}

	public static class MultiStatementNode extends CompositeNode {
		
	}

	public static class StatementNode extends CompositeNode {
		
		
		@Override
		void build(StringBuilder builder) {
	
			super.build(builder);
			
			builder.append(';');
		}
	
	}

	public static class EscapeNode extends CompositeNode {
		
		@Override
		void build(StringBuilder builder) {
	
			builder.append('{');
			
			super.build(builder);
			
			builder.append('}');
		}
	
	}

	public static class PieceNode extends Node {
		
		private String text;
		
		PieceNode(String val) {
			this.text = val;
		}
	
		public boolean consume(Node node) {
			
			//If pieces are exactly the same class.. combine them
			if(getClass() == node.getClass()) {
				text += ((PieceNode)node).text;
				return true;
			}
			
			return false;
		}
	
		void build(StringBuilder builder) {
			builder.append(text);
		}
	
		@Override
		public String toString() {
			return getClass().getSimpleName() + ": " + text;
		}
		
		
	}

	public static class GrammarPiece extends PieceNode {
		
		GrammarPiece(String val) {
			super(val);
		}
		
	}

	public static class ParameterPiece extends PieceNode {
		
		int idx;
	
		ParameterPiece(int idx) {
			super("$" + idx);
		}
		
	}

	public static class UnquotedIdentifierPiece extends PieceNode {
	
		UnquotedIdentifierPiece(String val) {
			super(val);
		}
		
	}

	public static class QuotedIdentifierPiece extends PieceNode {
	
		QuotedIdentifierPiece(String val) {
			super(val);
		}
		
	}

	public static class StringLiteralPiece extends PieceNode {
	
		StringLiteralPiece(String val) {
			super(val);
		}
		
	}

	public static class CommentPiece extends PieceNode {
		
		CommentPiece(String val) {
			super(val);
		}
		
	}

	public static class WhitespacePiece extends PieceNode {
		
		WhitespacePiece(String val) {
			super(val);
		}
		
	}

}
