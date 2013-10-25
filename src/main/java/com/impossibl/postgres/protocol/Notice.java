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
package com.impossibl.postgres.protocol;

public class Notice {

  public static final String SUCCESS_CLASS                    = "00";
  public static final String WARNING_CLASS                    = "01";
  public static final String NO_DATA_CLASS                    = "02";
  public static final String STATEMENT_INCOMPLETE_CLASS       = "03";
  public static final String CONNECTION_EXC_CLASS             = "08";
  public static final String TRIGGERED_ACTION_EXC_CLASS       = "09";
  public static final String FEATURE_NOT_SUPPORTED_CLASS      = "0A";
  public static final String INVALID_TXN_INIT_CLASS           = "0B";
  public static final String LOCATOR_EXC_CLASS                = "0F";
  public static final String INVALID_GRANTOR_CLASS            = "0L";
  public static final String INVALID_ROLE_SPEC_CLASS          = "0P";
  public static final String DIAGNOSTICS_EXC_CLASS            = "0Z";
  public static final String CASE_NOT_FOUNC_CLASS             = "20";
  public static final String CARDINALITY_VIOL_CLASS           = "21";
  public static final String DATA_EXC_CLASS                   = "22";
  public static final String INTEGRITY_CONST_VIOL_CLASS       = "23";
  public static final String INVALID_CURSOR_STATE_CLASS       = "24";
  public static final String INVALID_TXN_STATE_CLASS          = "25";
  public static final String INVALID_STATEMENT_NAME_CLASS     = "26";
  public static final String TRIGGERED_DATA_CHANGE_VIOL_CLASS = "27";
  public static final String INVALID_AUTHZN_SPEC_CLASS        = "28";
  public static final String DEP_DESCRIPTOR_EXISTS_CLASS      = "2B";
  public static final String INVALID_TXN_TERM_CLASS           = "2D";
  public static final String SQL_ROUTINE_EXC_CLASS            = "2F";
  public static final String INVALID_CURSOR_NAME_CLASS        = "34";
  public static final String EXT_ROUTINE_EXC_CLASS            = "38";
  public static final String EXT_ROUTINE_INV_EXC_CLASS        = "39";
  public static final String SAVEPOINT_EXC_CLASS              = "3B";
  public static final String INVALID_CATALOG_NAME_CLASS       = "3D";
  public static final String INVALID_SCHEMA_NAME_CLASS        = "3F";
  public static final String TRANSACTION_ROLLBACK_CLASS       = "40";
  public static final String SYNTAX_OR_ACCESS_ERROR_CLASS     = "42";
  public static final String CHECK_VIOL_CLASS                 = "44";
  public static final String INSUFFICIENT_RESOURCES_CLASS     = "53";
  public static final String PROGRAM_LIMIT_EXCEEDED_CLASS     = "54";
  public static final String OBJECT_PREREQ_STATE__CLASS       = "55";
  public static final String OPERATOR_INTERVENTION_CLASS      = "57";
  public static final String SYSTEM_ERROR_CLASS               = "58";
  public static final String CONFIG_ERROR_CLASS               = "F0";
  public static final String FOREIGN_DATA_EXC_CLASS           = "HV";
  public static final String PL_PGSQL_CLASS                   = "P0";
  public static final String INTERNAL_ERROR_CLASS             = "XX";


  private String severity;
  private String code;
  private String message;
  private String detail;
  private String hint;
  private String position;
  private String where;
  private String routine;
  private String file;
  private String line;
  private String schema;
  private String table;
  private String column;
  private String datatype;
  private String constraint;


  public Notice() {
  }

  public Notice(String severity, String code, String message) {
    this.severity = severity;
    this.code = code;
    this.message = message;
  }

  public boolean isSuccess() {
    return code != null && code.startsWith(SUCCESS_CLASS);
  }

  public boolean isWarning() {
    return code != null &&
        (code.startsWith(WARNING_CLASS) || code.startsWith(NO_DATA_CLASS));
  }

  public boolean isError() {
    return code != null && !isSuccess() && !isWarning();
  }

  public String getSeverity() {
    return severity;
  }

  public void setSeverity(String severity) {
    this.severity = severity;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getDetail() {
    return detail;
  }

  public void setDetail(String detail) {
    this.detail = detail;
  }

  public String getHint() {
    return hint;
  }

  public void setHint(String hint) {
    this.hint = hint;
  }

  public String getPosition() {
    return position;
  }

  public void setPosition(String position) {
    this.position = position;
  }

  public String getWhere() {
    return where;
  }

  public void setWhere(String where) {
    this.where = where;
  }

  public String getRoutine() {
    return routine;
  }

  public void setRoutine(String routine) {
    this.routine = routine;
  }

  public String getFile() {
    return file;
  }

  public void setFile(String file) {
    this.file = file;
  }

  public String getLine() {
    return line;
  }

  public void setLine(String line) {
    this.line = line;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getColumn() {
    return column;
  }

  public void setColumn(String column) {
    this.column = column;
  }

  public String getDatatype() {
    return datatype;
  }

  public void setDatatype(String datatype) {
    this.datatype = datatype;
  }

  public String getConstraint() {
    return constraint;
  }

  public void setConstraint(String constraint) {
    this.constraint = constraint;
  }

}
