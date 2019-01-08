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
package com.impossibl.postgres.api.jdbc;

import com.impossibl.postgres.system.Version;
import com.impossibl.postgres.types.Type;

import java.sql.JDBCType;

public enum PGType implements PGAnyType {

  //CHECKSTYLE:OFF: ParenPad|NoWhitespaceBefore

  BOOL                    (  16,  "bool",         JDBCType.BOOLEAN),

  BYTES                   (  17,  "bytea",        JDBCType.BINARY),

  INT2                    (  21,  "int2",         JDBCType.SMALLINT),
  INT4                    (  23,  "int4",         JDBCType.INTEGER),
  INT8                    (  20,  "int8",         JDBCType.BIGINT),
  FLOAT4                  ( 700,  "float4",       JDBCType.REAL),
  FLOAT8                  ( 701,  "float8",       JDBCType.DOUBLE),
  MONEY                   ( 790,  "money",        JDBCType.DECIMAL),
  NUMERIC                 (1700,  "numeric",      JDBCType.NUMERIC),

  CHAR                    (  18,  "char",         JDBCType.CHAR),
  NAME                    (  19,  "name",         JDBCType.VARCHAR),
  TEXT                    (  25,  "text",         JDBCType.VARCHAR),
  BPCHAR                  (1042,  "bpchar",       JDBCType.VARCHAR),
  VARCHAR                 (1043,  "varchar",      JDBCType.VARCHAR),
  CSTRING                 (2275,  "cstring",      JDBCType.VARCHAR),

  JSON                    ( 114,  "json",         JDBCType.VARCHAR,                   "9.1"),
  JSONB                   (3802,  "jsonb",        JDBCType.VARCHAR,                   "9.4"),
  XML                     ( 142,  "xml",          JDBCType.SQLXML),

  DATE                    (1082,  "date",         JDBCType.DATE),
  TIME                    (1083,  "time",         JDBCType.TIME),
  TIME_WITH_TIMEZONE      (1266,  "timetz",       JDBCType.TIME_WITH_TIMEZONE),
  TIMESTAMP               (1114,  "timestamp",    JDBCType.TIMESTAMP),
  TIMESTAMP_WITH_TIMEZONE (1184,  "timestamptz",  JDBCType.TIMESTAMP_WITH_TIMEZONE),
  INTERVAL                (1186,  "interval",     JDBCType.OTHER),
  ABSTIME                 ( 702,  "abstime",      JDBCType.OTHER),
  RELTIME                 ( 703,  "reltime",      JDBCType.OTHER),
  TINTERVAL               ( 704,  "tinterval",    JDBCType.OTHER),

  POINT                   ( 600,  "point",        JDBCType.OTHER),
  LINE_SEGMENT            ( 601,  "lseg",         JDBCType.OTHER),
  PATH                    ( 602,  "path",         JDBCType.OTHER),
  BOX                     ( 603,  "box",          JDBCType.OTHER),
  POLYGON                 ( 604,  "polygon",      JDBCType.OTHER),
  LINE                    ( 628,  "line",         JDBCType.OTHER),
  CIRCLE                  ( 718,  "circle",       JDBCType.OTHER),

  MACADDR                 ( 829,  "macaddr",      JDBCType.OTHER),
  MACADDR8                ( 774,  "macaddr8",     JDBCType.OTHER,                     "10.0"),
  CIDR                    ( 650,  "cidr",         JDBCType.OTHER),
  INET                    ( 869,  "inet",         JDBCType.OTHER),

  BIT                     (1560,  "bit",          JDBCType.BOOLEAN),
  VARBIT                  (1562,  "varbit",       JDBCType.VARBINARY),

  RECORD                  (2249,  "record",       JDBCType.STRUCT),

  UUID                    (2950,  "uuid",         JDBCType.OTHER),

  ACL_ITEM                (1033,  "aclitem",      JDBCType.OTHER),

  OID                     (  26,  "oid",          JDBCType.INTEGER),
  TID                     (  27,  "tid",          JDBCType.ROWID),
  XID                     (  28,  "xid",          JDBCType.OTHER),
  CID                     (  29,  "cid",          JDBCType.OTHER),

  HSTORE                  (null, "hstore",        JDBCType.OTHER),
  CITEXT                  (null, "citext",        JDBCType.VARCHAR),

  ;

  //CHECKSTYLE:ON: ParenPad|NoWhitespaceBefore

  private Integer oid;
  private String name;
  private JDBCType jdbcType;
  private Version requiredVersion;

  PGType(Integer oid, String name, JDBCType jdbcType, String requiredVersion) {
    this.oid = oid;
    this.name = name;
    this.jdbcType = jdbcType;
    this.requiredVersion = Version.parse(requiredVersion);
  }

  PGType(Integer oid, String name, JDBCType jdbcType) {
    this(oid, name, jdbcType, "0.0");
  }

  @Override
  public Version getRequiredVersion() {
    return requiredVersion;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getVendor() {
    return VENDOR_NAME;
  }

  @Override
  public Integer getVendorTypeNumber() {
    return oid;
  }

  public JDBCType getMappedType() {

    return jdbcType;
  }

  public static PGType valueOf(Type type) {

    for (PGType pgType : values()) {
      if (pgType.oid != null && pgType.oid == type.getId())
        return pgType;
    }

    return null;
  }

  public static PGType valueOf(int oid) {

    for (PGType pgType : values()) {
      if (pgType.oid != null && pgType.oid == oid)
        return pgType;
    }

    throw new IllegalArgumentException("PostgreSQL Type:" + oid + " is not a valid PGType value.");
  }

}
