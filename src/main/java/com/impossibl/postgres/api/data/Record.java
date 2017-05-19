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
package com.impossibl.postgres.api.data;

import com.impossibl.postgres.types.Type;

import java.util.Arrays;
import java.util.Objects;

import static java.util.Objects.requireNonNull;



public class Record {

  String typeName;
  Type[] attributeTypes;
  Object[] attributeValues;

  public Record(String typeName, Type[] attributeTypes, Object[] attributeValues) {
    this.typeName = typeName;
    this.attributeTypes = attributeTypes;
    this.attributeValues = requireNonNull(attributeValues);
  }

  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  public Type[] getAttributeTypes() {
    return attributeTypes;
  }

  public void setAttributeTypes(Type[] attributeTypes) {
    this.attributeTypes = attributeTypes;
  }

  public Object[] getAttributeValues() {
    return attributeValues;
  }

  public void setAttributeValues(Object[] attributeValues) {
    this.attributeValues = attributeValues;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Record record = (Record) o;
    return Objects.equals(typeName, record.typeName) &&
        Arrays.equals(attributeTypes, record.attributeTypes) &&
        Arrays.equals(attributeValues, record.attributeValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeName, attributeTypes, attributeValues);
  }

}
