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

import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.TypeRef;

public class ResultField implements FieldFormatRef {

  private String name;
  private int relationId;
  private int relationAttributeNumber;
  private TypeRef typeRef;
  private short typeLength;
  private int typeModifier;
  private FieldFormat format;

  public ResultField(String name, int relationId, short relationAttributeNumber, Type type, short typeLength, int typeModifier, FieldFormat format) {
    this.name = name;
    this.relationId = relationId;
    this.relationAttributeNumber = relationAttributeNumber;
    this.typeRef = type;
    this.typeLength = typeLength;
    this.typeModifier = typeModifier;
    this.format = format;
  }

  public ResultField(String name, int relationId, short relationAttributeNumber, TypeRef typeRef, short typeLength, int typeModifier, FieldFormat format) {
    this.name = name;
    this.relationId = relationId;
    this.relationAttributeNumber = relationAttributeNumber;
    this.typeRef = typeRef;
    this.typeLength = typeLength;
    this.typeModifier = typeModifier;
    this.format = format;
  }

  public String getName() {
    return name;
  }

  public int getRelationId() {
    return relationId;
  }

  public int getRelationAttributeNumber() {
    return relationAttributeNumber;
  }

  public TypeRef getTypeRef() {
    return typeRef;
  }

  public short getTypeLength() {
    return typeLength;
  }

  public int getTypeModifier() {
    return typeModifier;
  }

  public void setFormat(FieldFormat v) {
    format = v;
  }

  public FieldFormat getFormat() {
    return format;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(name);
    if (relationId != 0) {
      sb.append(String.format(" (%s:%d)", relationId, relationAttributeNumber));
    }
    sb.append(" : ");
    sb.append(typeRef != null ? typeRef.toString() : "<unknown>");
    return sb.toString();
  }

}
