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

public class Exceptions {

  public static final SQLException NOT_SUPPORTED = new SQLException("Operation not supported");
  public static final SQLException NOT_IMPLEMENTED = new SQLException("Operation not implemented");
  public static final SQLException NOT_ALLOWED_ON_PREP_STMT = new SQLException("Operation not allowed on PreparedStatement");
  public static final SQLException INVALID_COMMAND_FOR_GENERATED_KEYS = new SQLException("SQL command does not support generated keys");
  public static final SQLException NO_RESULT_SET_AVAILABLE = new SQLException("No result set available");
  public static final SQLException NO_RESULT_COUNT_AVAILABLE = new SQLException("No result count available");
  public static final SQLException ILLEGAL_ARGUMENT = new SQLException("Illegal argument");
  public static final SQLException CLOSED_STATEMENT = new SQLException("Statement closed");
  public static final SQLException CLOSED_RESULT_SET = new SQLException("Result set closed");
  public static final SQLException CLOSED_CONNECTION = new SQLException("Connection closed");
  public static final SQLException CLOSED_BLOB = new SQLException("Blob closed");
  public static final SQLException INVALID_COLUMN_NAME = new SQLException("Invalid column name");
  public static final SQLException COLUMN_INDEX_OUT_OF_BOUNDS = new SQLException("Column index out of bounds");
  public static final SQLException ROW_INDEX_OUT_OF_BOUNDS = new SQLException("Row index out of bounds");
  public static final SQLException PARAMETER_INDEX_OUT_OF_BOUNDS = new SQLException("Parameter index out of bounds");
  public static final SQLException SERVER_VERSION_NOT_SUPPORTED = new SQLException("Server version not supported");
  public static final SQLException UNWRAP_ERROR = new SQLException("Unwrap error");
  public static final SQLException CURSOR_NOT_SCROLLABLE = new SQLException("Cursor not scrollable");

}
