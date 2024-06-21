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

import com.impossibl.postgres.api.data.ACLItem;
import com.impossibl.postgres.api.data.CidrAddr;
import com.impossibl.postgres.api.data.InetAddr;
import com.impossibl.postgres.api.data.Interval;
import com.impossibl.postgres.api.data.Path;
import com.impossibl.postgres.api.data.Tid;
import com.impossibl.postgres.system.Version;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.TypeLiteral;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

public enum PGType implements PGAnyType {

  //CHECKSTYLE:OFF: ParenPad|NoWhitespaceBefore

  BOOL                    (  16,  "bool",         Boolean.class,        JDBCType.BOOLEAN),

  BYTES                   (  17,  "bytea",        InputStream.class,    JDBCType.BINARY),

  INT2                    (  21,  "int2",         Short.class,          JDBCType.SMALLINT),
  INT4                    (  23,  "int4",         Integer.class,        JDBCType.INTEGER),
  INT8                    (  20,  "int8",         Long.class,           JDBCType.BIGINT),
  FLOAT4                  ( 700,  "float4",       Float.class,          JDBCType.REAL),
  FLOAT8                  ( 701,  "float8",       Double.class,         JDBCType.DOUBLE),
  MONEY                   ( 790,  "money",        BigDecimal.class,     JDBCType.DECIMAL),
  NUMERIC                 (1700,  "numeric",      BigDecimal.class,     JDBCType.NUMERIC),

  CHAR                    (  18,  "char",         String.class,         JDBCType.CHAR),
  NAME                    (  19,  "name",         String.class,         JDBCType.VARCHAR),
  TEXT                    (  25,  "text",         String.class,         JDBCType.VARCHAR),
  TEXT_ARRAY              (1009,  "text[]",       String[].class,       JDBCType.OTHER),
  BPCHAR                  (1042,  "bpchar",       String.class,         JDBCType.VARCHAR),
  VARCHAR                 (1043,  "varchar",      String.class,         JDBCType.VARCHAR),
  CSTRING                 (2275,  "cstring",      String.class,         JDBCType.VARCHAR),

  JSON                    ( 114,  "json",         String.class,         JDBCType.VARCHAR, "9.1"),
  JSONB                   (3802,  "jsonb",        String.class,         JDBCType.VARCHAR, "9.4"),
  XML                     ( 142,  "xml",          SQLXML.class,         JDBCType.SQLXML),

  DATE                    (1082,  "date",         Date.class,           JDBCType.DATE),
  TIME                    (1083,  "time",         Time.class,           JDBCType.TIME),
  TIME_WITH_TIMEZONE      (1266,  "timetz",       Time.class,           JDBCType.TIME_WITH_TIMEZONE),
  TIMESTAMP               (1114,  "timestamp",    Timestamp.class,      JDBCType.TIMESTAMP),
  TIMESTAMP_WITH_TIMEZONE (1184,  "timestamptz",  Timestamp.class,      JDBCType.TIMESTAMP_WITH_TIMEZONE),
  INTERVAL                (1186,  "interval",     Interval.class,       JDBCType.OTHER),

  POINT                   ( 600,  "point",        double[].class,       JDBCType.OTHER),
  LINE_SEGMENT            ( 601,  "lseg",         double[].class,       JDBCType.OTHER),
  PATH                    ( 602,  "path",         Path.class,           JDBCType.OTHER),
  BOX                     ( 603,  "box",          double[].class,       JDBCType.OTHER),
  POLYGON                 ( 604,  "polygon",      double[][].class,     JDBCType.OTHER),
  LINE                    ( 628,  "line",         double[].class,       JDBCType.OTHER),
  CIRCLE                  ( 718,  "circle",       double[].class,       JDBCType.OTHER),

  MACADDR                 ( 829,  "macaddr",      byte[].class,         JDBCType.OTHER),
  MACADDR8                ( 774,  "macaddr8",     byte[].class,         JDBCType.OTHER, "10.0"),
  CIDR                    ( 650,  "cidr",         CidrAddr.class,       JDBCType.OTHER),
  INET                    ( 869,  "inet",         InetAddr.class,       JDBCType.OTHER),

  BIT                     (1560,  "bit",          boolean[].class,      JDBCType.BOOLEAN),
  VARBIT                  (1562,  "varbit",       boolean[].class,      JDBCType.VARBINARY),

  RECORD                  (2249,  "record",       Struct.class,         JDBCType.STRUCT),

  UUID                    (2950,  "uuid",         java.util.UUID.class, JDBCType.OTHER),

  ACL_ITEM                (1033,  "aclitem",      ACLItem.class,        JDBCType.OTHER),

  OID                     (  26,  "oid",          Integer.class,        JDBCType.INTEGER),
  TID                     (  27,  "tid",          Tid.class,            JDBCType.ROWID),
  XID                     (  28,  "xid",          Integer.class,        JDBCType.OTHER),
  CID                     (  29,  "cid",          Integer.class,        JDBCType.OTHER),

  HSTORE                  (null, "hstore",        Types.HSTORE,         JDBCType.OTHER),
  CITEXT                  (null, "citext",        String.class,         JDBCType.VARCHAR),

  ;

  //CHECKSTYLE:ON: ParenPad|NoWhitespaceBefore

  private Integer oid;
  private String name;
  private Class<?> javaType;
  private JDBCType jdbcType;
  private Version requiredVersion;

  PGType(Integer oid, String name, Class<?> javaType, JDBCType jdbcType, String requiredVersion) {
    this.oid = oid;
    this.name = name;
    this.javaType = javaType;
    this.jdbcType = jdbcType;
    this.requiredVersion = Version.parse(requiredVersion);
  }

  PGType(Integer oid, String name, Class<?> javaType, JDBCType jdbcType) {
    this(oid, name, javaType, jdbcType, "0.0");
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

  @Override
  public Class<?> getJavaType() {
    return javaType;
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


  private static class Types {

    static final Class<Map<String, String>> HSTORE = new TypeLiteral<Map<String, String>>() { }.getRawType();

  }

}
