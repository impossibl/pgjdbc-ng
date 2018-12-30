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

import com.impossibl.postgres.system.ServerConnectionInfo;
import com.impossibl.postgres.system.ServerInfo;
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
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
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
public class SharedRegistry {

  public interface Factory {

    SharedRegistry get(ServerConnectionInfo info);

  }

  private static Logger logger = Logger.getLogger(SharedRegistry.class.getName());

  private static class ProcSharingKey {
    private ServerInfo serverInfo;
    private ClassLoader classLoader;

    ProcSharingKey(ServerInfo serverInfo, ClassLoader classLoader) {
      this.serverInfo = serverInfo;
      this.classLoader = classLoader;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ProcSharingKey that = (ProcSharingKey) o;
      return serverInfo.equals(that.serverInfo) &&
          classLoader.equals(that.classLoader);
    }

    @Override
    public int hashCode() {
      return Objects.hash(serverInfo, classLoader);
    }

  }

  private static final Map<ProcSharingKey, Procs> sharedProcs = new HashMap<>();

  private TreeMap<Integer, Type> oidMap;
  private Map<QualifiedName, Type> nameMap;
  private TreeMap<Integer, Type> relIdMap;
  private Procs procs;

  private AtomicBoolean seeded = new AtomicBoolean(false);
  private ReadWriteLock lock = new ReentrantReadWriteLock();


  public SharedRegistry(ServerInfo serverInfo, ClassLoader classLoader) {

    synchronized (SharedRegistry.class) {
      this.procs = sharedProcs.computeIfAbsent(
          new ProcSharingKey(serverInfo, classLoader),
          key -> new Procs(key.serverInfo, key.classLoader)
      );
    }

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

    oidMap.put(2205,  new BaseType(2205, "regclass",  (short) 4,  (byte) 4, Category.Numeric,  ',', 2210, procs, Binary, Binary));
    oidMap.put(2206,  new BaseType(2206, "regtype",   (short) 4,  (byte) 4, Category.Numeric,  ',', 2211, procs, Binary, Binary));
    oidMap.put(2210, new ArrayType(2210, "_regclass", (short)-1,  (byte) 4, Category.Array,  ',', 0, procs, Binary, Binary, oidMap.get(2205)));
    oidMap.put(2211, new ArrayType(2211, "_regtype",  (short)-1,  (byte) 4, Category.Array,  ',', 0, procs, Binary, Binary, oidMap.get(2206)));

    nameMap = new HashMap<>();
    oidMap.values().forEach(type -> nameMap.put(type.getQualifiedName(), type));

    relIdMap = new TreeMap<>();
  }

  /**
   * Loads a type by its type-id (aka OID)
   *
   * @param typeId The type's id
   * @return Type object or null, if none found
   */
  public Type findOrLoadType(int typeId, Registry.TypeLoader loader) throws IOException {

    if (typeId == 0)
      return null;

    lock.readLock().lock();
    try {

      Type type = oidMap.get(typeId);
      if (type == null) {

        lock.readLock().unlock();
        try {

          type = loader.load(typeId);

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
   * Finds a type by its qualified name
   *
   * @param name The type's qualified name
   * @return Type object or null, if none found
   */
  public Type findOrLoadType(QualifiedName name, Registry.TypeLoader loader) throws IOException {

    if (name == null)
      return null;

    lock.readLock().lock();
    try {

      Type type = nameMap.get(name);
      if (type == null) {

        lock.readLock().unlock();
        try {

          type = loader.load(name);

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
  public CompositeType findOrLoadRelationType(int relationId, Registry.TypeLoader loader) throws IOException {

    if (relationId == 0)
      return null;

    lock.readLock().lock();
    try {

      CompositeType type = (CompositeType) relIdMap.get(relationId);
      if (type == null) {

        lock.readLock().unlock();
        try {

          type = loader.loadRelation(relationId);

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

    void seed(SharedRegistry registry) throws IOException;

  }

  public boolean seed(Seeder seeder) throws IOException {

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
    nameMap.put(type.getQualifiedName(), type);
    if (type.getRelationId() != 0) {
      relIdMap.put(type.getRelationId(), type);
    }
  }

  /**
   * Loads a matching Codec given the proc-name of its encoder and decoder
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
   * Loads a matching Codec given the proc-name of its encoder and decoder
   *
   * @param encoderName proc-name of the encoder
   * @param decoderName proc-name of the decoder
   * @return A matching Codec instance
   */
  Type.BinaryCodec loadBinaryCodec(String encoderName, String decoderName) {
    return new Type.BinaryCodec(
        loadDecoderProc(decoderName, DEFAULT_BINARY_DECODER, ByteBuf.class),
        loadEncoderProc(encoderName, DEFAULT_BINARY_ENCODER, ByteBuf.class)
    );
  }

  /*
   * Loads a matching encoder given its proc-name
   */
  private <Buffer> Codec.Encoder<Buffer> loadEncoderProc(String procName, Codec.Encoder<Buffer> defaultEncoder, Class<? extends Buffer> bufferType) {

    return procs.loadEncoderProc(procName, defaultEncoder, bufferType);
  }

  /*
   * Loads a matching decoder given its proc-name
   */
  private <Buffer> Codec.Decoder<Buffer> loadDecoderProc(String procName, Codec.Decoder<Buffer> defaultDecoder, Class<? extends Buffer> bufferType) {

    return procs.loadDecoderProc(procName, defaultDecoder, bufferType);
  }

  /*
   * Loads a matching modifier parser given mod-in name
   */
  Modifiers.Parser loadModifierParser(String modInName) {

    return procs.loadModifierParserProc(modInName);
  }

}
