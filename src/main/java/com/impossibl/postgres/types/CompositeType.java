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
package com.impossibl.postgres.types;

import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * A database composite type.
 *
 * @author kdubb
 *
 */
public class CompositeType extends Type {

  /**
   *  An attribute of the composite type.
   */
  public static class Attribute {

    private int number;
    private String name;
    private Type type;
    private boolean nullable;
    private boolean autoIncrement;
    private boolean hasDefault;
    private Map<String, Object> typeModifiers;

    Attribute(int number, String name, Type type,
              boolean nullable, boolean autoIncrement,
              boolean hasDefault, Map<String, Object> typeModifiers) {
      this.number = number;
      this.name = name;
      this.type = type;
      this.nullable = nullable;
      this.autoIncrement = autoIncrement;
      this.hasDefault = hasDefault;
      this.typeModifiers = typeModifiers;
    }

    public int getNumber() {
      return number;
    }

    public String getName() {
      return name;
    }

    public Type getType() {
      return type;
    }

    public boolean isNullable() {
      return nullable;
    }

    public boolean isAutoIncrement() {
      return autoIncrement;
    }

    public boolean isHasDefault() {
      return hasDefault;
    }

    public Map<String, Object> getTypeModifiers() {
      return typeModifiers;
    }

    @Override
    public String toString() {
      return name + " : " + type;
    }

  }

  private List<Attribute> attributes;

  public Attribute getAttribute(int number) {

    //First try the obvious
    if (number > 0 && number <= attributes.size() && attributes.get(number - 1).number == number) {
      return attributes.get(number - 1);
    }

    //Now search all
    for (Attribute attr : attributes) {
      if (attr.number == number) {
        return attr;
      }
    }

    return null;
  }

  public List<Attribute> getAttributes() {
    return attributes;
  }

  public Type[] getAttributesTypes() {
    Type[] attributeTypes = new Type[attributes.size()];
    for (int c = 0; c < attributes.size(); ++c) {
      attributeTypes[c] = attributes.get(c).type;
    }
    return attributeTypes;
  }

  @Override
  public boolean isParameterFormatSupported(FieldFormat format) {

    boolean allSupported = super.isParameterFormatSupported(format);
    for (Attribute attr : attributes) {
      allSupported &= attr.type.isParameterFormatSupported(format);
    }

    return allSupported;
  }

  @Override
  public boolean isResultFormatSupported(FieldFormat format) {

    boolean allSupported = super.isResultFormatSupported(format);
    for (Attribute attr : attributes) {
      allSupported &= attr.type.isResultFormatSupported(format);
    }

    return allSupported;
  }

  @Override
  public void load(PgType.Row pgType, Collection<com.impossibl.postgres.system.tables.PgAttribute.Row> pgAttrs, Context context, SharedRegistry registry) {

    super.load(pgType, pgAttrs, context, registry);

    if (pgAttrs == null) {

      attributes = Collections.emptyList();
    }
    else {

      attributes = new ArrayList<>(pgAttrs.size());

      for (PgAttribute.Row pgAttr : pgAttrs) {
        Type type = registry.loadType(pgAttr.getTypeId(), context::loadType);
        Attribute attr = new Attribute(pgAttr.getNumber(),
                                       pgAttr.getName(),
                                       type,
                                       pgAttr.isNullable(),
                                       pgAttr.isAutoIncrement(),
                                       pgAttr.isHasDefault(),
                                       type != null ? type.getModifierParser().parse(pgAttr.getTypeModifier()) : emptyMap());

        attributes.add(attr);
      }

      attributes.sort((o1, o2) -> {
        int o1n = o1.number < 0 ? o1.number + Integer.MAX_VALUE : o1.number;
        int o2n = o2.number < 0 ? o2.number + Integer.MAX_VALUE : o2.number;
        return o1n - o2n;
      });

    }

  }

}
