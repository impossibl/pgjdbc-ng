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

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.TypeMapContext;
import com.impossibl.postgres.types.Type;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;

public abstract class PGStruct implements Struct {

  protected Context context;
  protected String typeName;
  protected Type[] attributeTypes;

  PGStruct(Context context, String typeName, Type[] attributeTypes) {
    super();
    this.context = context;
    this.typeName = typeName;
    this.attributeTypes = attributeTypes;
  }

  public Type[] getAttributeTypes() {
    return attributeTypes;
  }

  @Override
  public String getSQLTypeName() {
    return typeName;
  }

  @Override
  public Object[] getAttributes() throws SQLException {
    try {
      return getAttributes(context);
    }
    catch (IOException e) {
      throw new SQLException(e);
    }
  }

  public Object[] getAttributes(Map<String, Class<?>> typeMap) throws SQLException {
    try {
      return getAttributes(new TypeMapContext(context, typeMap));
    }
    catch (IOException e) {
      throw new SQLException(e);
    }
  }

  public abstract Object[] getAttributes(Context context) throws IOException;

}
