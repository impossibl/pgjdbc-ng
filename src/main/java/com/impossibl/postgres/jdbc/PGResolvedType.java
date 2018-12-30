package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.api.jdbc.PGAnyType;
import com.impossibl.postgres.types.Type;


class PGResolvedType implements PGAnyType {

  private Type type;

  PGResolvedType(Type type) {
    this.type = type;
  }

  @Override
  public String getName() {
    return type.getQualifiedName().toString();
  }

  @Override
  public String getVendor() {
    return VENDOR_NAME;
  }

  @Override
  public Integer getVendorTypeNumber() {
    return type.getId();
  }

}
