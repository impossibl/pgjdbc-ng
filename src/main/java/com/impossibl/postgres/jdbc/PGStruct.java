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

import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerce;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.mapGetType;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.CompositeType.Attribute;

public class PGStruct implements Struct {

  PGConnection connection;
  CompositeType type;
  Object[] values;

  public PGStruct(PGConnection connection, CompositeType type, Object[] values) {
    super();
    this.connection = connection;
    this.type = type;
    this.values = values;
  }

  public CompositeType getType() {
    return type;
  }

  @Override
  public String getSQLTypeName() throws SQLException {
    return type.getName();
  }

  @Override
  public Object[] getAttributes() throws SQLException {

    return getAttributes(connection.getTypeMap());
  }

  @Override
  public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {

    Object[] newValues = new Object[values.length];

    List<Attribute> attrs = type.getAttributes();

    for(int c=0; c < attrs.size(); c++) {

      Attribute attr = attrs.get(c);

      Class<?> targetType = mapGetType(type, map, connection);

      newValues[c] = coerce(values[c], attr.type, targetType, map, connection);
    }

    return newValues;
  }

}
