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

import com.impossibl.postgres.protocol.Notice;

import static com.impossibl.postgres.utils.guava.Strings.nullToEmpty;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Iterator;
import java.util.List;

/**
 * Utilities for creating SQLException and SQLWarnings from PostgreSQL's
 * "Notice" and "Error" message data
 *
 * @author kdubb
 *
 */
public class ErrorUtils {

  /**
   * Converts the given list of notices into a chained list of SQLWarnings
   *
   * @param notices List of notices to convert
   * @return Root of converted list or null is no notices were given
   */
  public static SQLWarning makeSQLWarningChain(List<Notice> notices) {

    Iterator<Notice> noticeIter = notices.iterator();

    SQLWarning root = null;

    if (noticeIter.hasNext()) {

      root = makeSQLWarning(noticeIter.next());
      SQLWarning current = root;

      while (noticeIter.hasNext()) {

        Notice notice = noticeIter.next();

        // Only include warnings...
        if (!notice.isWarning())
          continue;

        SQLWarning nextWarning = makeSQLWarning(notice);
        current.setNextWarning(nextWarning);
        current = nextWarning;
      }

    }

    return root;
  }

  /**
   * Converts the given list of notices into a chained list of SQLExceptions
   *
   * @param notices List of notices to convert
   * @return Root of converted list or null is no notices were given
   */
  public static SQLException makeSQLExceptionChain(List<Notice> notices) {

    Iterator<Notice> noticeIter = notices.iterator();

    SQLException root = null;

    if (noticeIter.hasNext()) {

      root = makeSQLException("", noticeIter.next());
      SQLException current = root;

      while (noticeIter.hasNext()) {

        SQLException nextException = makeSQLException("", noticeIter.next());
        current.setNextException(nextException);
        current = nextException;
      }

    }

    return root;
  }

  /**
   * Converts a single warning notice to a single SQLWarning
   *
   * @param notice Notice to convert
   * @return SQLWarning
   */
  public static SQLWarning makeSQLWarning(Notice notice) {

    if (!notice.isWarning()) {
      throw new IllegalArgumentException("notice not an error");
    }

    return new SQLWarning(notice.getMessage(), notice.getCode());
  }

  /**
   * Converts a single error notice to a single SQLException
   *
   * @param notice Notice to convert
   * @return SQLException
   */
  public static SQLException makeSQLException(Notice notice) {
    return makeSQLException("", notice);
  }

  /**
   * Converts a single error notice to a single SQLException
   *
   * @param notice
   *          Notice to convert
   * @return SQLException
   */
  public static SQLException makeSQLException(String message, Notice notice) {

    PGSQLExceptionInfo e;

    String code = notice.getCode();

    if (code.startsWith("23")) {

      e = new PGSQLIntegrityConstraintViolationException(message + nullToEmpty(notice.getMessage()), notice.getCode());

    }
    else {

      e = new PGSQLSimpleException(message + nullToEmpty(notice.getMessage()), notice.getCode());

    }

    //Copy extended error information (9.3+)
    e.setSchema(notice.getSchema());
    e.setTable(notice.getTable());
    e.setColumn(notice.getColumn());
    e.setDatatype(notice.getDatatype());
    e.setConstraint(notice.getConstraint());

    return (SQLException) e;
  }

  /**
   * Creates a single chain of warnings. The method attempts to append to the
   * base chain but if no base is provided it returns the add chain.
   *
   * @param base Base warning chain
   * @param add Warning chain to append to base
   * @return Head of the new complete warning chain
   */
  public static SQLWarning chainWarnings(SQLWarning base, SQLWarning add) {

    if (base == null)
      return add;

    SQLWarning current = base;
    while (current.getNextWarning() != null) {
      current = current.getNextWarning();
    }

    current.setNextWarning(add);

    return base;
  }

}
