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
