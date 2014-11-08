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

import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.system.procs.Procs;
import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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

    public int number;
    public String name;
    public Type type;
    public boolean nullable;
    public boolean autoIncrement;
    public boolean hasDefault;
    public Map<String, Object> typeModifiers;

    @Override
    public String toString() {
      return name + " : " + type;
    }

  }

  private List<Attribute> attributes;

  public CompositeType(int id, String name, int arrayTypeId, String procName, Procs procs) {
    super(id, name, null, null, Category.Composite, ',', arrayTypeId, procs.loadNamedBinaryCodec(procName, null), procs.loadNamedTextCodec(procName, null));
  }

  public CompositeType(int id, String name, int arrayTypeId, Procs procs) {
    this(id, name, arrayTypeId, "record_", procs);
  }

  public CompositeType() {
  }

  public Attribute getAttribute(int number) {

    //First try the obvious
    if (number > 0 && attributes.get(number - 1).number == number) {
      return attributes.get(number - 1);
    }

    //Now search all
    for (int c = 0, sz = attributes.size(); c < sz; ++c) {
      Attribute attr = attributes.get(c);
      if (attr.number == number) {
        return attr;
      }
    }

    return null;
  }

  public List<Attribute> getAttributes() {
    return attributes;
  }

  public void setAttributes(List<Attribute> attributes) {
    this.attributes = attributes;
  }

  @Override
  public boolean isParameterFormatSupported(Format format) {

    boolean allSupported = super.isParameterFormatSupported(format);
    for (Attribute attr : attributes) {
      allSupported &= attr.type.isParameterFormatSupported(format);
    }

    return allSupported;
  }

  @Override
  public boolean isResultFormatSupported(Format format) {

    boolean allSupported = super.isResultFormatSupported(format);
    for (Attribute attr : attributes) {
      allSupported &= attr.type.isResultFormatSupported(format);
    }

    return allSupported;
  }

  @Override
  public Class<?> getJavaType(Format format, Map<String, Class<?>> customizations) {

    Class<?> type = (customizations != null) ? customizations.get(getName()) : null;
    if (type == null) {
      type = super.getJavaType(format, customizations);
    }

    return type;
  }

  @Override
  public void load(PgType.Row pgType, Collection<com.impossibl.postgres.system.tables.PgAttribute.Row> pgAttrs, Registry registry) {

    super.load(pgType, pgAttrs, registry);

    if (pgAttrs == null) {

      attributes = Collections.emptyList();
    }
    else {

      attributes = new ArrayList<>(pgAttrs.size());

      for (PgAttribute.Row pgAttr : pgAttrs) {

        Attribute attr = new Attribute();
        attr.number = pgAttr.number;
        attr.name = pgAttr.name;
        attr.type = registry.loadType(pgAttr.typeId);
        attr.nullable = pgAttr.nullable;
        attr.hasDefault = pgAttr.hasDefault;
        attr.typeModifiers = attr.type != null ? attr.type.getModifierParser().parse(pgAttr.typeModifier) : Collections.<String, Object>emptyMap();
        attr.autoIncrement = pgAttr.autoIncrement;

        attributes.add(attr);
      }

      Collections.sort(attributes, new Comparator<Attribute>() {

        @Override
        public int compare(Attribute o1, Attribute o2) {
          int o1n = o1.number < 0 ? o1.number + Integer.MAX_VALUE : o1.number;
          int o2n = o2.number < 0 ? o2.number + Integer.MAX_VALUE : o2.number;
          return o1n - o2n;
        }

      });

    }

  }

}
