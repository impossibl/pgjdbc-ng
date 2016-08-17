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
package com.impossibl.postgres.utils;

import java.io.Serializable;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class TypeLiteral<T> implements Serializable {

  private static final long serialVersionUID = 1L;

  private transient Type actualType;

  protected TypeLiteral() {
  }

  /**
   * @return the actual type represented by this object
   */
  public final Type getType() {
    if (actualType == null) {
      Class<?> typeLiteralSubclass = getTypeLiteralSubclass(this.getClass());
      if (typeLiteralSubclass == null) {
        throw new RuntimeException(getClass() + " is not a subclass of TypeLiteral");
      }
      actualType = getTypeParameter(typeLiteralSubclass);
      if (actualType == null) {
        throw new RuntimeException(getClass() + " does not specify the type parameter T of TypeLiteral<T>");
      }
    }
    return actualType;
  }

  /**
   * @return the raw type represented by this object
   */
  @SuppressWarnings("unchecked")
  public final Class<T> getRawType() {
    Type type = getType();
    if (type instanceof Class) {
      return (Class<T>) type;
    }
    else if (type instanceof ParameterizedType) {
      return (Class<T>) ((ParameterizedType) type).getRawType();
    }
    else if (type instanceof GenericArrayType) {
      return (Class<T>) Object[].class;
    }
    else {
      throw new RuntimeException("Illegal type");
    }
  }

  private static Class<?> getTypeLiteralSubclass(Class<?> clazz) {
    Class<?> superclass = clazz.getSuperclass();
    if (superclass.equals(TypeLiteral.class)) {
      return clazz;
    }
    else if (superclass.equals(Object.class)) {
      return null;
    }
    else {
      return (getTypeLiteralSubclass(superclass));
    }
  }

  private static Type getTypeParameter(Class<?> superclass) {
    Type type = superclass.getGenericSuperclass();
    if (type instanceof ParameterizedType) {
      ParameterizedType parameterizedType = (ParameterizedType) type;
      if (parameterizedType.getActualTypeArguments().length == 1) {
        return parameterizedType.getActualTypeArguments()[0];
      }
    }
    return null;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TypeLiteral<?>) {
      TypeLiteral<?> that = (TypeLiteral<?>) obj;
      return this.getType().equals(that.getType());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getType().hashCode();
  }

  @Override
  public String toString() {
    return getType().toString();
  }

}
