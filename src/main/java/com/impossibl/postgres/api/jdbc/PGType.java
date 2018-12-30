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

public enum PGType implements PGAnyType {

  BOOL                    (  16,  "bool"),

  BYTES                   (  17,  "bytea"),

  INT2                    (  21,  "int2"),
  INT4                    (  23,  "int4"),
  INT8                    (  20,  "int8"),
  FLOAT4                  ( 700,  "float4"),
  FLOAT8                  ( 701,  "float8"),
  MONEY                   ( 790,  "money"),
  NUMERIC                 (1700,  "numeric"),

  CHAR                    (  18,  "char"),
  NAME                    (  19,  "name"),
  TEXT                    (  25,  "text"),
  BPCHAR                  (1042,  "bpchar"),
  VARCHAR                 (1043,  "varchar"),
  CSTRING                 (2275,  "cstring"),

  JSON                    ( 114,  "json"),
  JSONB                   (3802,  "jsonb"),
  XML                     ( 142,  "xml"),

  DATE                    (1082,  "date"),
  TIME                    (1083,  "time"),
  TIME_WITH_TIMEZONE      (1266,  "timetz"),
  TIMESTAMP               (1114,  "timestamp"),
  TIMESTAMP_WITH_TIMEZONE (1184,  "timestamptz"),
  INTERVAL                (1186,  "interval"),
  ABSTIME                 ( 702,  "abstime"),
  RELTIME                 ( 703,  "reltime"),
  TINTERVAL               ( 704,  "tinterval"),

  POINT                   ( 600,  "point"),
  LINE_SEGMENT            ( 601,  "lseg"),
  PATH                    ( 602,  "path"),
  BOX                     ( 603,  "box"),
  POLYGON                 ( 604,  "polygon"),
  LINE                    ( 628,  "line"),
  CIRCLE                  ( 718,  "circle"),

  MACADDR                 ( 829,  "macaddr"),
  MACADDR8                ( 774,  "macaddr8"),
  CIDR                    ( 650,  "cidr"),
  INET                    ( 869,  "inet"),

  BIT                     (1560,  "bit"),
  VARBIT                  (1562,  "varbit"),

  RECORD                  (2249,  "record"),

  UUID                    (2950,  "uuid"),

  ACL_ITEM                (1033,  "aclitem"),

  OID                     (  26,  "oid"),
  TID                     (  27,  "tid"),
  XID                     (  28,  "xid"),
  CID                     (  29,  "cid"),

  HSTORE                  (null, "hstore"),
  CITEXT                  (null,  ""),

  ;

  private int oid;
  private String name;

  PGType(Integer oid, String name) {
    this.oid = oid;
    this.name = name;
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

}
