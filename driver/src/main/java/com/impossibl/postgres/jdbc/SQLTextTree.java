/**
 * Copyright (c) 2013, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.impossibl.postgres.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;



public class SQLTextTree {

  public interface Processor {

    Node process(Node node) throws SQLException;

  }

  public abstract static class Node {

    private int startPos;
    private int endPos;

    public Node(int startPos, int endPos) {
      this.startPos = startPos;
      this.endPos = endPos;
    }

    public Node copy() {
      return this;
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
      return processor.process(this);
    }

    void removeAll(final Class<? extends Node> nodeType, boolean recurse) {
      try {
        process(new Processor() {

          @Override
          public Node process(Node node) throws SQLException {
            if (nodeType.isInstance(node))
              return null;
            return node;
          }
        }, recurse);
      }
      catch (SQLException e) {
        // Ignore
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

    public CompositeNode(List<Node> nodes, int startPos) {
      super(startPos, -1);
      this.nodes = nodes;
    }

    @Override
    public Node copy() {
      CompositeNode clone = new CompositeNode(getStartPos());
      copyNodes(clone);
      return clone;
    }

    protected void copyNodes(CompositeNode newNode) {
      newNode.nodes = new ArrayList<>(nodes.size());
      for (Node node : nodes) {
        newNode.nodes.add((Node) node.copy());
      }
    }

    @Override
    void build(StringBuilder builder) {

      for (Node node : nodes) {
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

    @Override
    public Node process(Processor processor, boolean recurse) throws SQLException {

      //Process each child node...
      ListIterator<Node> nodeIter = nodes.listIterator();
      while (nodeIter.hasNext()) {

        Node res;
        if (recurse) {
          res = nodeIter.next().process(processor, recurse);
        }
        else {
          res = processor.process(nodeIter.next());
        }

        if (res != null) {
          nodeIter.set(res);
        }
        else {
          nodeIter.remove();
        }
      }

      return processor.process(this);
    }

    void add(Node node) {

      this.nodes.add(node);
    }

    public boolean containsAll(Class<? extends Node> cls) {

      for (Node node : nodes) {
        if (!cls.isInstance(node))
          return false;
      }

      return true;
    }

    public void trim() {

      if (nodes.isEmpty())
        return;

      //Prune starting and ending whitespace
      if (nodes.get(0) instanceof WhitespacePiece) {
        nodes.remove(0);
      }

      if (nodes.isEmpty()) {
        return;
      }

      if (nodes.get(nodes.size() - 1) instanceof WhitespacePiece) {
        nodes.remove(nodes.size() - 1);
      }

    }

    public Node getFirstNode() {
      if (nodes.isEmpty())
        return null;
      return nodes.get(0);
    }

    public Node getLastNode() {
      if (nodes.isEmpty())
        return null;
      return nodes.get(nodes.size() - 1);
    }

  }

  public static class MultiStatementNode extends CompositeNode {

    public MultiStatementNode(int startPos) {
      super(startPos);
    }

    public MultiStatementNode(List<Node> nodes, int startPos) {
      super(nodes, startPos);
    }

    @Override
    public Node copy() {
      MultiStatementNode clone = new MultiStatementNode(getStartPos());
      copyNodes(clone);
      return clone;
    }

    @Override
    void build(StringBuilder builder) {

      Iterator<Node> nodeIter = iterator();
      while (nodeIter.hasNext()) {
        nodeIter.next().build(builder);
        if (nodeIter.hasNext())
          builder.append(';');
      }
    }

  }

  public static class StatementNode extends CompositeNode {

    public StatementNode(int startPos) {
      super(startPos);
    }

    @Override
    public Node copy() {
      StatementNode clone = new StatementNode(getStartPos());
      copyNodes(clone);
      return clone;
    }
  }

  public static class EscapeNode extends CompositeNode {

    public EscapeNode(int startPos) {
      super(startPos);
    }

    @Override
    public Node copy() {
      EscapeNode clone = new EscapeNode(getStartPos());
      copyNodes(clone);
      return clone;
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
    public Node copy() {
      ParenGroupNode clone = new ParenGroupNode(getStartPos());
      copyNodes(clone);
      return clone;
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

    void coalesce(PieceNode otherPiece) {
      setText(getText() + otherPiece.getText());
      setEndPos(getStartPos() + getText().length());
    }

    @Override
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
      super("$" + idx, pos);
      this.idx = idx;
    }

    @Override
    public Node copy() {
      return new ParameterPiece(idx, getStartPos());
    }

    public int getIdx() {
      return idx;
    }

    public void setIdx(int idx) {
      this.idx = idx;
      setText("$" + idx);
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
