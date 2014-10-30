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

import java.sql.SQLException;
import java.text.ParseException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLText {

  private MultiStatementNode root;

  public SQLText(String sqlText) throws ParseException {
    root = parse(sqlText);
  }

  public SQLText(MultiStatementNode copyRoot) {
    root = copyRoot;
  }

  public SQLText copy() {
    return new SQLText((MultiStatementNode) root.copy());
  }

  public int getStatementCount() {
    if (root == null)
      return 0;
    return root.getNodeCount();
  }

  public StatementNode getFirstStatement() {
    if (root == null || root.getNodeCount() == 0)
      return null;
    return (StatementNode) root.get(0);
  }

  public StatementNode getLastStatement() {
    if (root == null || root.getNodeCount() == 0)
      return null;
    return (StatementNode) root.get(root.getNodeCount() - 1);
  }

  public void addStatements(SQLText sqlText) {
    root.nodes.addAll(sqlText.root.nodes);
  }

  public void process(Processor processor, boolean recurse) throws SQLException {
    root.process(processor, recurse);
  }

  @Override
  public String toString() {
    return root.toString();
  }

  public static MultiStatementNode parse(final String sql) throws ParseException {

    Deque<CompositeNode> parents = new LinkedList<>();

    parents.push(new MultiStatementNode(0));
    parents.push(new StatementNode(0));

    int paramId = 1;
    int ndx = 0;

    try {
      while (ndx < sql.length()) {

        char c = sql.charAt(ndx);
        switch (c) {
          case '\'':
            ndx = consumeStringLiteral(sql, ndx + 1, parents.peek());
            continue;
          case '"':
            ndx = consumeQuotedIdentifier(sql, ndx, parents.peek());
            continue;
          case '?':
            ParameterPiece parameterPiece = new ParameterPiece(paramId++, ndx);
            parents.peek().add(parameterPiece);
            break;
          case '$':
            ndx = consumeDollar(sql, ndx, parents.peek());
            continue;
          case '(':
          case ')':
            ndx = consumeParens(sql, ndx, parents);
            continue;
          case '{':
          case '}':
            ndx = consumeBraces(sql, ndx, parents);
            continue;
          case '/':
            if (lookAhead(sql, ndx) == '*') {
              ndx = consumeMultilineComment(sql, ndx, parents.peek());
              continue;
            }
            else {
              parents.peek().add(new GrammarPiece("/", ndx));
              break;
            }
          case '-':
            if (lookAhead(sql, ndx) == '-') {
              ndx = consumeSinglelineComment(sql, ndx, parents.peek());
              continue;
            }
            else if (Character.isDigit(lookAhead(sql, ndx))) {
              ndx = consumeNumeric(sql, ndx, parents.peek());
              continue;
            }
            else {
              GrammarPiece grammarPiece = new GrammarPiece("-", ndx);
              parents.peek().add(grammarPiece);
              break;
            }
          case ';':
            if (parents.size() == 2) {
              paramId = 1;
              CompositeNode comp = parents.pop();
              comp.setEndPos(ndx);
              parents.peek().add(comp);
              parents.push(new StatementNode(ndx));
            }
            else {
              parents.peek().add(new GrammarPiece(";", ndx));
            }
            break;
          default:
            if (Character.isWhitespace(c)) {
              WhitespacePiece whitespacePiece = new WhitespacePiece(sql.substring(ndx, ndx + 1), ndx);
              if (parents.peek().getLastNode() instanceof WhitespacePiece) {
                ((WhitespacePiece) parents.peek().getLastNode()).coalesce(whitespacePiece);
              }
              else {
                parents.peek().add(whitespacePiece);
              }
            }
            else if (Character.isDigit(c) || (c == '+' && Character.isDigit(lookAhead(sql, ndx)))) {
              ndx = consumeNumeric(sql, ndx, parents.peek());
              continue;
            }
            else if (Character.isJavaIdentifierStart(c)) {
              ndx = consumeUnquotedIdentifier(sql, ndx, parents.peek());
              continue;
            }
            else {
              GrammarPiece grammarPiece = new GrammarPiece(sql.substring(ndx, ndx + 1), ndx);
              if (parents.peek().getLastNode() instanceof GrammarPiece) {
                ((GrammarPiece) parents.peek().getLastNode()).coalesce(grammarPiece);
              }
              else {
                parents.peek().add(grammarPiece);
              }
            }
        }

        ++ndx;
      }

      // Auto close last statement
      if (parents.peek() instanceof StatementNode) {

        StatementNode stmt = (StatementNode) parents.peek();

        stmt.trim();

        if (stmt.getNodeCount() > 0) {
          CompositeNode tmp = parents.pop();
          tmp.setEndPos(ndx);
          parents.peek().add(tmp);
        }
      }

      if (parents.peek() instanceof StatementNode == false && parents.peek() instanceof MultiStatementNode == false) {
        throw new IllegalArgumentException("error parsing SQL");
      }

      return (MultiStatementNode) parents.getLast();
    }
    catch (ParseException e) {
      throw e;
    }
    catch (Exception e) {
      String errorTxt = sql.substring(ndx, Math.min(sql.length(), ndx + 10));
      throw new ParseException("Error near: " + errorTxt, ndx);
    }
  }

  private static int consumeNumeric(final String sql, final int start, final CompositeNode parent) throws ParseException {
    Matcher matcher = Pattern.compile("((?:[+-]?(?:\\d+)?(?:\\.\\d+(?:[eE][+-]?\\d+)?))|(?:[+-]?\\d+))").matcher(sql.substring(start));
    if (matcher.find()) {
      parent.add(new NumericLiteralPiece(matcher.group(1), matcher.start()));
      return start + matcher.group(1).length();
    }
    else {
      throw new ParseException("Invalid numeric literal", start);
    }
  }

  private static int consumeUnquotedIdentifier(final String sql, final int start, final CompositeNode parent) {
    int ndx = start;
    char c;
    do {
      c = lookAhead(sql, ndx++);
    }
    while (ndx < sql.length() && Character.isJavaIdentifierPart(c));

    parent.add(new UnquotedIdentifierPiece(sql.substring(start, ndx), start));

    return ndx;
  }

  private static int consumeBraces(final String sql, final int start, final Deque<CompositeNode> parents) throws ParseException {
    if (sql.charAt(start) == '{') {
      parents.push(new EscapeNode(start));
    }
    else {
      if (parents.peek() instanceof EscapeNode) {
        EscapeNode tmp = (EscapeNode) parents.pop();
        tmp.setEndPos(start);
        parents.peek().add(tmp);
      }
      else {
        throw new ParseException("Mismatched curly brace", start);
      }
    }

    return start + 1;
  }

  private static int consumeParens(final String sql, final int start, final Deque<CompositeNode> parents) throws ParseException {
    if (sql.charAt(start) == '(') {
      parents.push(new ParenGroupNode(start));
    }
    else {
      if (parents.peek() instanceof ParenGroupNode) {
        ParenGroupNode tmp = (ParenGroupNode) parents.pop();
        tmp.setEndPos(start);
        parents.peek().add(tmp);
      }
      else {
        throw new ParseException("Mismmatched parenthesis", start);
      }
    }

    return start + 1;
  }

  private static int consumeDollar(final String sql, final int start, final CompositeNode parent) throws ParseException {
    int ndx = start;
    do {
      if (lookAhead(sql, ndx) == '$') {
        final String ident = sql.substring(start, ndx + 2);
        final int pos = sql.indexOf(ident, ndx + 2);
        if (pos != -1) {
          String quotedText = sql.substring(ndx + 2, pos);
          parent.add(new StringLiteralPiece(quotedText, ident, start));
          return pos + ident.length();
        }
        else {
          ++ndx;
          break;
        }
      }
    } while (++ndx < sql.length());

    // Just treat as a grammar piece
    parent.add(new GrammarPiece(sql.substring(start, ndx), start));
    return ndx;
  }

  private static int consumeStringLiteral(final String sql, final int start, final CompositeNode parent) throws ParseException {
    int ndx = start;
    do {
      char c = sql.charAt(ndx);
      if (c == '\'') {
        if (sql.charAt(ndx - 1) == '\\') {  // look-behind
          // skip escaped
        }
        else {
          break;
        }
      }

      if (lookAhead(sql, ndx) == 0) {
        throw new ParseException("Unterminated string literal", start);
      }

      ++ndx;
    } while (true);

    StringLiteralPiece literalPiece = new StringLiteralPiece(sql.substring(start, ndx), start);
    parent.add(literalPiece);

    return ndx + 1;
  }

  private static int consumeQuotedIdentifier(final String sql, final int start, final CompositeNode parent) throws ParseException {
    int ndx = start + 1;
    do {
      char c = sql.charAt(ndx);
      if (c == '"') {
        if (sql.charAt(ndx - 1) == '"') {  // look-behind
          // skip escaped
        }
        else {
          break;
        }
      }

      if (lookAhead(sql, ndx) == 0) {
        throw new ParseException("Unterminated string literal", start);
      }

      ++ndx;
    } while (true);

    QuotedIdentifierPiece literalPiece = new QuotedIdentifierPiece(sql.substring(start + 1, ndx), start);
    parent.add(literalPiece);

    return ndx + 1;
  }

  private static int consumeSinglelineComment(final String sql, final int start, final CompositeNode parent) {
    int ndx = start + 2;
    do {
      char c = sql.charAt(ndx);
      if (c == '\r' || c == '\n') {
        ndx = (lookAhead(sql, ndx) == '\n') ? ndx + 2 : ndx + 1;
        break;
      }

    } while (++ndx < sql.length());

    CommentPiece commentPiece = new CommentPiece(sql.substring(start, ndx), start);
    parent.add(commentPiece);

    return ndx;
  }

  private static int consumeMultilineComment(final String sql, final int start, final CompositeNode parent) throws ParseException {
    int nestLevel = 1;
    int ndx = start + 1;
    do {
      char c = lookAhead(sql, ndx);
      if (c == 0) {
        throw new ParseException("Unterminated comment", start);
      }

      if (c == '/' && lookAhead(sql, ndx + 1) == '*') {
        ++nestLevel;
        ++ndx;
      }
      else if (c == '*' && lookAhead(sql, ndx + 1) == '/') {
        --nestLevel;
        ++ndx;
      }

      ++ndx;
    } while (nestLevel > 0);

    CommentPiece commentPiece = new CommentPiece(sql.substring(start, ndx + 1), start);
    parent.add(commentPiece);

    return ndx + 1;
  }

  private static char lookAhead(final String sql, final int ndx) {
    if (ndx + 1 < sql.length()) {
      return sql.charAt(ndx + 1);
    }

    return 0;
  }
}
