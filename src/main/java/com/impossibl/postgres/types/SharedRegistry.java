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
import com.impossibl.postgres.system.NoticeException;
import com.impossibl.postgres.system.procs.Procs;
import com.impossibl.postgres.types.Type.Category;
import com.impossibl.postgres.types.Type.Codec;

import static com.impossibl.postgres.protocol.FieldFormat.Binary;
import static com.impossibl.postgres.system.procs.Procs.DEFAULT_BINARY_DECODER;
import static com.impossibl.postgres.system.procs.Procs.DEFAULT_BINARY_ENCODER;
import static com.impossibl.postgres.system.procs.Procs.DEFAULT_TEXT_DECODER;
import static com.impossibl.postgres.system.procs.Procs.DEFAULT_TEXT_ENCODER;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.logging.Logger;

import io.netty.buffer.ByteBuf;

/**
 * Storage and loading for all the known types of a given context.
 *
 * @author kdubb
 *
 */
public class SharedRegistry {

  private static Logger logger = Logger.getLogger(SharedRegistry.class.getName());

  private TreeMap<Integer, Type> oidMap;
  private Map<String, Type> nameMap;
  private TreeMap<Integer, Type> relIdMap;
  private Map<String, String> typeNameAliases;
  private Procs procs;

  private AtomicBoolean seeded = new AtomicBoolean(false);
  private ReadWriteLock lock = new ReentrantReadWriteLock();


  public SharedRegistry(ClassLoader classLoader) {

    this.procs = new Procs(classLoader);

    // Required initial types for bootstrapping
    oidMap = new TreeMap<>();
    oidMap.put(16,  new BaseType(16, "bool",       (short) 1,  (byte) 1, Category.Boolean, ',', 1000, procs, Binary, Binary));
    oidMap.put(17,  new BaseType(17, "bytea",      (short) 1,  (byte) 4, Category.User,    ',', 1001, procs, Binary, Binary));
    oidMap.put(18,  new BaseType(18, "char",       (short) 1,  (byte) 1, Category.String,  ',', 1002, procs, Binary, Binary));
    oidMap.put(19,  new BaseType(19, "name",       (short)64,  (byte) 1, Category.String,  ',', 1003, procs, Binary, Binary));
    oidMap.put(20,  new BaseType(20, "int8",       (short) 8,  (byte) 8, Category.Numeric, ',', 1016, procs, Binary, Binary));
    oidMap.put(21,  new BaseType(21, "int2",       (short) 2,  (byte) 2, Category.Numeric, ',', 1005, procs, Binary, Binary));
    oidMap.put(22, new ArrayType(22, "int2vector", (short)-1,  (byte) 4, Category.Array,   ',', 1006, procs, Binary, Binary, oidMap.get(21)));
    oidMap.put(23,  new BaseType(23, "int4",       (short) 4,  (byte) 4, Category.Numeric, ',', 1007, procs, Binary, Binary));
    oidMap.put(24,  new BaseType(24, "regproc",    (short) 4,  (byte) 4, Category.Numeric, ',', 1008, procs, Binary, Binary));
    oidMap.put(25,  new BaseType(25, "text",       (short)-1,  (byte) 4, Category.String,  ',', 1009, procs, Binary, Binary));
    oidMap.put(26,  new BaseType(26, "oid",        (short) 4,  (byte) 4, Category.Numeric, ',', 1028, procs, Binary, Binary));
    oidMap.put(27,  new BaseType(27, "tid",        (short) 6,  (byte) 2, Category.User,    ',', 1010, procs, Binary, Binary));
    oidMap.put(28,  new BaseType(28, "xid",        (short) 4,  (byte) 4, Category.User,    ',', 1011, procs, Binary, Binary));
    oidMap.put(29,  new BaseType(29, "cid",        (short) 4,  (byte) 4, Category.User,    ',', 1012, procs, Binary, Binary));
    oidMap.put(30, new ArrayType(30, "oidvector",  (short)-1,  (byte) 4, Category.Array,   ',', 1013, procs, Binary, Binary, oidMap.get(26)));

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

  public Type loadType(TypeRef typeRef, Function<Integer, Type> loader) {
    if (typeRef instanceof Type) {
      return (Type) typeRef;
    }
    return loadType(typeRef.getOid(), loader);
  }

  /**
   * Loads a type by its type-id (aka OID)
   *
   * @param typeId The type's id
   * @return Type object or null, if none found
   */
  public Type loadType(int typeId, Function<Integer, Type> loader) {

    if (typeId == 0)
      return null;

    lock.readLock().lock();
    try {

      Type type = oidMap.get(typeId);
      if (type == null) {

        lock.readLock().unlock();
        try {

          type = loader.apply(typeId);

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
  public Type loadType(String name, Function<String, Type> loader) {

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

          type = loader.apply(name);

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
  public CompositeType loadRelationType(int relationId, Function<Integer, CompositeType> loader) {

    if (relationId == 0)
      return null;

    lock.readLock().lock();
    try {

      CompositeType type = (CompositeType) relIdMap.get(relationId);
      if (type == null) {

        lock.readLock().unlock();
        try {

          type = loader.apply(relationId);

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

  public interface Seeder {
    void seed(SharedRegistry registry) throws IOException, NoticeException;
  }

  public boolean seed(Seeder seeder) throws IOException, NoticeException {

    if (seeded.getAndSet(true)) {
      return false;
    }

    seeder.seed(this);

    return true;
  }

  public void addTypes(Collection<Type> types) {

    lock.writeLock().lock();
    try {

      for (Type type : types) {
        updateType(type);
      }

    }
    finally {
      lock.writeLock().unlock();
    }

  }

  private void updateType(Type type) {
    if (type == null) return;

    oidMap.put(type.getId(), type);
    nameMap.put(type.getName(), type);
    if (type.getRelationId() != 0) {
      relIdMap.put(type.getRelationId(), type);
    }
  }

  /**
   * Loads a matching Codec given the proc-id of its encoder and decoder
   *
   * @param encoderName proc-name of the encoder
   * @param decoderName proc-name of the decoder
   * @return A matching Codec instance
   */
  Type.TextCodec loadTextCodec(String encoderName, String decoderName, Context context) {
    return new Type.TextCodec(
        loadDecoderProc(decoderName, context, DEFAULT_TEXT_DECODER, CharSequence.class),
        loadEncoderProc(encoderName, context, DEFAULT_TEXT_ENCODER, StringBuilder.class)
    );
  }

  /**
   * Loads a matching Codec given the proc-id of its encoder and decoder
   *
   * @param encoderName proc-name of the encoder
   * @param decoderName proc-name of the decoder
   * @return A matching Codec instance
   */
  Type.BinaryCodec loadBinaryCodec(String encoderName, String decoderName, Context context) {
    return new Type.BinaryCodec(
        loadDecoderProc(decoderName, context, DEFAULT_BINARY_DECODER, ByteBuf.class),
        loadEncoderProc(encoderName, context, DEFAULT_BINARY_ENCODER, ByteBuf.class)
    );
  }

  /*
   * Loads a matching encoder given its proc-id
   */
  private <Buffer> Codec.Encoder<Buffer> loadEncoderProc(String procName, Context context, Codec.Encoder<Buffer> defaultEncoder, Class<? extends Buffer> bufferType) {

    return procs.loadEncoderProc(procName, context, defaultEncoder, bufferType);
  }

  /*
   * Loads a matching decoder given its proc-id
   */
  private <Buffer> Codec.Decoder<Buffer> loadDecoderProc(String procName, Context context, Codec.Decoder<Buffer> defaultDecoder, Class<? extends Buffer> bufferType) {

    return procs.loadDecoderProc(procName, context, defaultDecoder, bufferType);
  }

  /*
   * Loads a matching parser given mod-in and mod-out ids
   */
  Modifiers.Parser loadModifierParser(String modInName, Context context) {

    return procs.loadModifierParserProc(modInName, context);
  }

}
