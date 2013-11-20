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

import com.impossibl.postgres.api.jdbc.PGSQLExceptionInfo;

import java.sql.SQLIntegrityConstraintViolationException;

public class PGSQLIntegrityConstraintViolationException extends SQLIntegrityConstraintViolationException implements PGSQLExceptionInfo {

  private static final long serialVersionUID = 1343560391090040431L;

  private String schema;
  private String table;
  private String column;
  private String datatype;
  private String constraint;

  public PGSQLIntegrityConstraintViolationException() {
    super();
  }

  public PGSQLIntegrityConstraintViolationException(String reason, String SQLState, int vendorCode, Throwable cause) {
    super(reason, SQLState, vendorCode, cause);
  }

  public PGSQLIntegrityConstraintViolationException(String reason, String SQLState, int vendorCode) {
    super(reason, SQLState, vendorCode);
  }

  public PGSQLIntegrityConstraintViolationException(String reason, String SQLState, Throwable cause) {
    super(reason, SQLState, cause);
  }

  public PGSQLIntegrityConstraintViolationException(String reason, String SQLState) {
    super(reason, SQLState);
  }

  public PGSQLIntegrityConstraintViolationException(String reason, Throwable cause) {
    super(reason, cause);
  }

  public PGSQLIntegrityConstraintViolationException(String reason) {
    super(reason);
  }

  public PGSQLIntegrityConstraintViolationException(Throwable cause) {
    super(cause);
  }

  @Override
  public String getSchema() {
    return schema;
  }

  @Override
  public void setSchema(String schema) {
    this.schema = schema;
  }

  @Override
  public String getTable() {
    return table;
  }

  @Override
  public void setTable(String table) {
    this.table = table;
  }

  @Override
  public String getColumn() {
    return column;
  }

  @Override
  public void setColumn(String column) {
    this.column = column;
  }

  @Override
  public String getDatatype() {
    return datatype;
  }

  @Override
  public void setDatatype(String datatype) {
    this.datatype = datatype;
  }

  @Override
  public String getConstraint() {
    return constraint;
  }

  @Override
  public void setConstraint(String constraint) {
    this.constraint = constraint;
  }

}
