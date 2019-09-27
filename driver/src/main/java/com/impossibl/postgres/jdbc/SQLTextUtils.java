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

import com.impossibl.postgres.jdbc.SQLTextTree.GrammarPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.StatementNode;

import static com.impossibl.postgres.system.Identifier.quoteIfNeeded;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;

import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;

/**
 * Utility functions for creating and transforming SQL text into and out of
 * PostgreSQL's native dialect.
 *
 * @author kdubb
 *
 */
class SQLTextUtils {

  /**
   * Tests the given value for equality to "true"
   *
   * @param value Value to test
   * @return true if the value is true
   */
  public static boolean isTrue(String value) {
    return "on".equals(value);
  }

  /**
   * Test the given value for equality to "false"
   *
   * @param value Value to test
   * @return true if the value is false
   */
  public static boolean isFalse(String value) {
    return "off".equals(value);
  }

  /**
   * Translates a JDBC isolation code to text
   *
   * @param level Isolation level code to translate
   * @return Text version of the given level code
   * @throws RuntimeException
   *          If the level code is not recognized
   */
  public static String getIsolationLevelText(int level) {

    switch (level) {
      case TRANSACTION_READ_UNCOMMITTED:
        return "READ UNCOMMITTED";
      case TRANSACTION_READ_COMMITTED:
        return "READ COMMITTED";
      case TRANSACTION_REPEATABLE_READ:
        return "REPEATABLE READ";
      case TRANSACTION_SERIALIZABLE:
        return "SERIALIZABLE";
    }

    throw new RuntimeException("unknown isolation level");
  }

  /**
   * Translates a text isolation level to a JDBC code
   *
   * @param level Name of level to translate
   * @return Isolation level code
   * @throws RuntimeException
   *          If the level is not recognized
   */
  public static int getIsolationLevel(String level) {

    switch (level.toUpperCase()) {
      case "READ UNCOMMITTED":
        return TRANSACTION_READ_UNCOMMITTED;
      case "READ COMMITTED":
        return TRANSACTION_READ_COMMITTED;
      case "REPEATABLE READ":
        return TRANSACTION_REPEATABLE_READ;
      case "SERIALIZABLE":
        return TRANSACTION_SERIALIZABLE;
    }

    throw new RuntimeException("unknown isolation level");
  }

  /**
   * Retrieves an SQL query for getting the current session's readability state
   *
   * @return SQL text
   */
  public static String getGetSessionReadabilityText() {

    return "SHOW default_transaction_read_only";
  }

  /**
   * Retrieves an SQL query for setting the current session's readability state
   *
   * @param readOnly
   * @return SQL text
   */
  public static String getSetSessionReadabilityText(boolean readOnly) {

    return "SET SESSION CHARACTERISTICS AS TRANSACTION " + (readOnly ? "READ ONLY" : "READ WRITE");
  }

  /**
   * Retrieves an SQL query for getting the current session's isolation level
   *
   * @return
   */
  public static String getGetSessionIsolationLevelText() {

    return "SHOW default_transaction_isolation";
  }

  /**
   * Retrieves an SQL query for setting the current session's isolation level
   *
   * @param level
   * @return SQL text
   */
  public static String getSetSessionIsolationLevelText(int level) {

    return "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL " + getIsolationLevelText(level);
  }

  /**
   * Retrieves text for beginning a transaction
   *
   * @return SQL text
   */
  public static String getBeginText() {
    return "BEGIN";
  }

  /**
   * Retrieves text for committing a transaction
   *
   * @return SQL text
   */
  public static String getCommitText() {
    return "COMMIT";
  }

  /**
   * Retrieves text for rolling back a transaction
   *
   * @return SQL text
   */
  public static String getRollbackText() {
    return "ROLLBACK";
  }

  /**
   * Retrieves text for rolling back a transaction to a specific savepoint
   *
   * @param savepoint Name of savepoint to rollback to
   * @return SQL text
   */
  public static String getRollbackToText(PGSavepoint savepoint) {
    return "ROLLBACK TO SAVEPOINT " + savepoint.getId();
  }

  /**
   * Retrieves text for setting a specific savepoint
   *
   * @return SQL text
   * @return
   */
  public static String getSetSavepointText(PGSavepoint savepoint) {
    return "SAVEPOINT " + savepoint.getId();
  }

  /**
   * Retrieves text for releasing a specific savepoint
   *
   * @param savepoint
   * @return
   */
  public static String getReleaseSavepointText(PGSavepoint savepoint) {
    return "RELEASE SAVEPOINT " + savepoint.getId();
  }

  /**
   * Prepends a clause, provided as text, to the given SQL text.
   *
   * @param sqlText
   *          Input SQL text
   * @return SQL text with prepended clause or null if sqlText cannot be
   *         prepended to
   */
  public static boolean prependClause(SQLText sqlText, String clause) {

    if (sqlText.getStatementCount() > 1)
      return false;

    StatementNode statement = sqlText.getLastStatement();

    statement.nodes.add(0, new GrammarPiece(clause, -1));

    return true;
  }

  public static boolean prependCursorDeclaration(SQLText sqlText, String cursorName, int resultSetType, int resultSetHoldability, boolean autoCommit) {

    if (sqlText.getStatementCount() > 1) {
      return false;
    }

    if (!sqlText.getFirstStatement().getFirstNode().toString().equalsIgnoreCase("SELECT")) {
      return false;
    }

    String preCursor = "DECLARE " + cursorName + " BINARY ";

    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
      preCursor += "SCROLL ";
    }
    else {
      preCursor += "NO SCROLL ";
    }

    preCursor += "CURSOR ";

    if (resultSetHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT || autoCommit) {
      preCursor += "WITH HOLD ";
    }

    preCursor += "FOR ";

    return prependClause(sqlText, preCursor);
  }

  /**
   * Appends a clause, provided as text, to the given SQL text.
   *
   * @param sqlText
   *          Input SQL text
   * @return SQL text with appended clause or null if sqlText cannot be appended
   *         to
   */
  public static boolean appendClause(SQLText sqlText, String clause) {

    if (sqlText.getStatementCount() > 1)
      return false;

    StatementNode statement = sqlText.getLastStatement();

    statement.add(new GrammarPiece(clause, -1));

    return true;
  }

  /**
   * Checks a statement node for an existing RETURNING clause
   * @param statement Statement to check for clause
   * @return True if statement contains RETURNING
   */
  public static boolean hasReturningClause(StatementNode statement) {

    for (SQLTextTree.Node node : statement.nodes) {

      if (node instanceof SQLTextTree.UnquotedIdentifierPiece &&
          ((SQLTextTree.UnquotedIdentifierPiece) node).getText().equals("RETURNING")) {
        return true;
      }

    }

    return false;
  }

  /**
   * Appends a RETURNING clause, containing only the provided columns, to the
   * given SQL text.
   *
   * @param sqlText Input SQL text
   * @return SQL text with appended clause or null if sqlText cannot be
   *          appended to
   */
  public static boolean appendReturningClause(SQLText sqlText, List<String> columns) {

    if (hasReturningClause(sqlText.getLastStatement())) {
      return true;
    }

    return appendClause(sqlText, " RETURNING " + joinColumns(columns, " , "));
  }

  /**
   * Appends a RETURNING * clause to the given SQL text.
   *
   * @param sqlText Input SQL text
   * @return SQL text with appended clause or null if sqlText cannot be
   *          appended to
   */
  public static boolean appendReturningClause(SQLText sqlText) {

    if (hasReturningClause(sqlText.getLastStatement())) {
      return true;
    }

    return appendClause(sqlText, " RETURNING *");
  }

  /**
   * Joins a list of columns into a string
   *
   * @param columns List of columns to join
   * @param separator String to separate columns with
   * @return Joined representation of columns
   */
  public static String joinColumns(List<String> columns, String separator) {

    StringBuilder sb = new StringBuilder();
    Iterator<String> columnIter = columns.iterator();

    while (columnIter.hasNext()) {

      sb.append(quoteIfNeeded(columnIter.next()));

      if (columnIter.hasNext()) {
        sb.append(separator);
      }

    }

    return sb.toString();
  }

  /**
   * Escape the literal text.

   * @param value value to append
   * @param standardConformingStrings If the server has standard_conforming_strings enabled
   * @return Escaped version of {@code value} text
   */
  public static String escapeLiteral(String value, boolean standardConformingStrings) {
    StringBuilder builder = new StringBuilder(value.length() * 2);
    try {
      escapeLiteral(value, standardConformingStrings, builder);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    return builder.toString();
  }

  /**
   * Escape the literal text, appending new text to {@code out}
   *
   * @param value value to append
   * @param standardConformingStrings If the server has standard_conforming_strings enabled
   */
  private static void escapeLiteral(String value, boolean standardConformingStrings, Appendable out) throws IOException {
    if (standardConformingStrings) {
      // Escape only single-quotes.
      for (int i = 0; i < value.length(); ++i) {
        char ch = value.charAt(i);
        if (ch == '\'') {
          out.append('\'');
        }
        out.append(ch);
      }
    }
    else {
      // Escape backslashes and single-quotes
      for (int i = 0; i < value.length(); ++i) {
        char ch = value.charAt(i);
        if (ch == '\\' || ch == '\'') {
          out.append(ch);
        }
        out.append(ch);
      }
    }
  }

}
