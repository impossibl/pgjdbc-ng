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

import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerce;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.mapGetType;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class PGStruct implements Struct {

  PGConnectionImpl connection;
  String typeName;
  Type[] attributeTypes;
  Object[] attributeValues;

  public PGStruct(PGConnectionImpl connection, String typeName, Type[] attributeTypes, Object[] values) {
    super();
    this.connection = connection;
    this.typeName = typeName;
    this.attributeTypes = attributeTypes;
    this.attributeValues = values;
  }

  public Type[] getAttributeTypes() {
    return attributeTypes;
  }

  @Override
  public String getSQLTypeName() throws SQLException {
    return typeName;
  }

  @Override
  public Object[] getAttributes() throws SQLException {

    return getAttributes(connection.getTypeMap());
  }

  @Override
  public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {

    Object[] newValues = new Object[attributeValues.length];

    for (int c = 0; c < attributeTypes.length; c++) {

      Type attrType = attributeTypes[c];

      Class<?> targetType = mapGetType(attrType, map, connection);

      newValues[c] = coerce(attributeValues[c], attrType, targetType, map, connection);
    }

    return newValues;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PGStruct pgStruct = (PGStruct) o;
    return Objects.equals(connection, pgStruct.connection) &&
        Objects.equals(typeName, pgStruct.typeName) &&
        Arrays.equals(attributeTypes, pgStruct.attributeTypes) &&
        Arrays.equals(attributeValues, pgStruct.attributeValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connection, typeName, attributeTypes, attributeValues);
  }

}
