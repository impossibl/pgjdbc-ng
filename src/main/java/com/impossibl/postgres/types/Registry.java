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
import com.impossibl.postgres.system.procs.Procs;
import com.impossibl.postgres.types.Type.Category;
import com.impossibl.postgres.types.Type.Codec;

import static com.impossibl.postgres.protocol.FieldFormat.Binary;
import static com.impossibl.postgres.system.procs.Procs.DEFAULT_BINARY_DECODER;
import static com.impossibl.postgres.system.procs.Procs.DEFAULT_TEXT_DECODER;
import static com.impossibl.postgres.system.procs.Procs.DEFAULT_TEXT_ENCODER;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import io.netty.buffer.ByteBuf;

/**
 * Storage and loading for all the known types of a given context.
 *
 * @author kdubb
 *
 */
public class Registry {

  private static Logger logger = Logger.getLogger(Registry.class.getName());

  private TreeMap<Integer, Type> oidMap;
  private Map<String, Type> nameMap;
  private TreeMap<Integer, Type> relIdMap;

  private WeakReference<Context> context;
  private Procs procs;
  private ReadWriteLock lock = new ReentrantReadWriteLock();

  private Map<String, String> typeNameAliases;


  public Registry(Context context) {

    this.context = new WeakReference<>(context);

    this.procs = new Procs(context.getClass().getClassLoader());

    // Required initial types for bootstrapping
    oidMap = new TreeMap<>();
    oidMap.put(16,  new BaseType(16, "bool",       (short) 1,  (byte) 1, Category.Boolean, ',', 1000, procs, Binary, Binary));
    oidMap.put(17,  new BaseType(17, "bytea",      (short) 1,  (byte) 4, Category.User,    ',', 1001, procs, Binary, Binary));
    oidMap.put(18,  new BaseType(18, "char",       (short) 1,  (byte) 1, Category.String,  ',', 1002, procs, Binary, Binary));
    oidMap.put(19,  new BaseType(19, "name",       (short)64,  (byte) 1, Category.String,  ',', 1003, procs, Binary, Binary));
    oidMap.put(20,  new BaseType(20, "int8",       (short) 8,  (byte) 8, Category.Numeric, ',', 1016, procs, Binary, Binary));
    oidMap.put(21,  new BaseType(21, "int2",       (short) 2,  (byte) 2, Category.Numeric, ',', 1005, procs, Binary, Binary));
    oidMap.put(23,  new BaseType(23, "int4",       (short) 4,  (byte) 4, Category.Numeric, ',', 1007, procs, Binary, Binary));
    oidMap.put(25,  new BaseType(25, "text",       (short)-1,  (byte) 4, Category.String,  ',', 1009, procs, Binary, Binary));
    oidMap.put(26,  new BaseType(26, "oid",        (short) 4,  (byte) 4, Category.Numeric, ',', 1028, procs, Binary, Binary));

    nameMap = new HashMap<>();
    oidMap.values().forEach(type -> nameMap.put(type.getName(), type));

    relIdMap = new TreeMap<>();

    typeNameAliases = new HashMap<>();
    typeNameAliases.put("smallint", "int2");
    typeNameAliases.put("integer", "int4");
    typeNameAliases.put("bigint", "int8");
    typeNameAliases.put("decimal", "numeric");
    typeNameAliases.put("real", "float4");
    typeNameAliases.put("double precision", "float8");
    typeNameAliases.put("smallserial", "int2");
    typeNameAliases.put("serial", "int4");
    typeNameAliases.put("bigserial", "int8");
  }

  public Context getContext() {
    return context.get();
  }

  public Type loadType(TypeRef typeRef) {
    if (typeRef instanceof Type) {
      return (Type) typeRef;
    }
    return loadType(typeRef.getOid());
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

    lock.readLock().lock();
    try {

      Type type = oidMap.get(typeId);
      if (type == null) {

        lock.readLock().unlock();
        try {

          type = getContext().loadType(typeId);

          updateType(type);

        }
        finally {
          lock.readLock().lock();
        }

      }

      return type;
    }
    finally {
      lock.readLock().unlock();
    }

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

    if (name.endsWith("[]")) {

      String baseName = name.substring(0, name.length() - 2);

      baseName = typeNameAliases.getOrDefault(baseName, baseName);

      name = "_" + baseName;
    }
    else {

      name = typeNameAliases.getOrDefault(name, name);
    }


    lock.readLock().lock();
    try {

      Type type = nameMap.get(name);
      if (type == null) {

        lock.readLock().unlock();
        try {

          type = getContext().loadType(name);

          updateType(type);

        }
        finally {
          lock.readLock().lock();
        }

      }

      return type;
    }
    finally {
      lock.readLock().unlock();
    }

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

    lock.readLock().lock();
    try {

      CompositeType type = (CompositeType) relIdMap.get(relationId);
      if (type == null) {

        lock.readLock().unlock();
        try {

          type = getContext().loadRelationType(relationId);

          updateType(type);

        }
        finally {
          lock.readLock().lock();
        }

      }

      return type;
    }
    finally {
      lock.readLock().unlock();
    }

  }

  public void updateType(Type type) {
    if (type == null) return;

    oidMap.put(type.getId(), type);
    nameMap.put(type.getName(), type);
    if (type.getRelationId() != 0) {
      relIdMap.put(type.getRelationId(), type);
    }
  }

  public void updateTypes(Collection<Type> types) {

    for (Type type : types) {
      updateType(type);
    }

  }

  /**
   * Loads a matching Codec given the proc-id of its encoder and decoder
   *
   * @param encoderName proc-name of the encoder
   * @param decoderName proc-name of the decoder
   * @return A matching Codec instance
   */
  Type.TextCodec loadTextCodec(String encoderName, String decoderName) {
    return new Type.TextCodec(
        loadDecoderProc(decoderName, DEFAULT_TEXT_DECODER, CharSequence.class),
        loadEncoderProc(encoderName, DEFAULT_TEXT_ENCODER, StringBuilder.class)
    );
  }

  /**
   * Loads a matching Codec given the proc-id of its encoder and decoder
   *
   * @param encoderName proc-name of the encoder
   * @param decoderName proc-name of the decoder
   * @return A matching Codec instance
   */
  Type.BinaryCodec loadBinaryCodec(String encoderName, String decoderName) {
    return new Type.BinaryCodec(
        loadDecoderProc(decoderName, DEFAULT_BINARY_DECODER, ByteBuf.class),
        loadEncoderProc(encoderName, Procs.DEFAULT_BINARY_ENCODER, ByteBuf.class)
    );
  }

  /*
   * Loads a matching encoder given its proc-id
   */
  private <Buffer> Codec.Encoder<Buffer> loadEncoderProc(String procName, Codec.Encoder<Buffer> defaultEncoder, Class<? extends Buffer> bufferType) {

    return procs.loadEncoderProc(procName, getContext(), defaultEncoder, bufferType);
  }

  /*
   * Loads a matching decoder given its proc-id
   */
  private <Buffer> Codec.Decoder<Buffer> loadDecoderProc(String procName, Codec.Decoder<Buffer> defaultDecoder, Class<? extends Buffer> bufferType) {

    return procs.loadDecoderProc(procName, getContext(), defaultDecoder, bufferType);
  }

  /*
   * Loads a matching parser given mod-in and mod-out ids
   */
  Modifiers.Parser loadModifierParser(String modInName) {

    return procs.loadModifierParserProc(modInName, getContext());
  }

}
