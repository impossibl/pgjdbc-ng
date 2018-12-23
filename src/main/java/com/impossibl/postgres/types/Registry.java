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

import com.impossibl.postgres.protocol.TypeRef;
import com.impossibl.postgres.system.Context;

import java.lang.ref.WeakReference;

/**
 * Storage and loading for all the known types of a given context.
 *
 * @author kdubb
 *
 */
public class Registry {

  private WeakReference<Context> context;
  private SharedRegistry sharedRegistry;

  public Registry(Context context, SharedRegistry sharedRegistry) {
    this.context = new WeakReference<>(context);
    this.sharedRegistry = sharedRegistry;
  }

  public Type loadType(TypeRef typeRef) {
    if (typeRef instanceof Type) {
      return (Type) typeRef;
    }
    return loadType(typeRef.getOid());
  }

  public SharedRegistry getShared() {
    return sharedRegistry;
  }

  private Context getContext() {
    return context.get();
  }

  /**
   * Loads a type by its type-id (aka OID)
   *
   * @param typeId The type's id
   * @return Type object or null, if none found
   */
  public Type loadType(int typeId) {

    if (typeId == 0)
      return null;

    return sharedRegistry.loadType(typeId, getContext()::loadType);
  }

  /**
   * Loads a type by its name
   *
   * @param name The type's name
   * @return Type object or null, if none found
   */
  public Type loadType(String name) {

    if (name == null)
      return null;

    return sharedRegistry.loadType(name, getContext()::loadType);
  }

  /**
   * Loads a relation (aka table) type by its relation-id
   *
   * @param relationId Relation ID of the type to load
   * @return Relation type or null
   */
  public CompositeType loadRelationType(int relationId) {

    if (relationId == 0)
      return null;

    return sharedRegistry.loadRelationType(relationId, getContext()::loadRelationType);
  }

}
