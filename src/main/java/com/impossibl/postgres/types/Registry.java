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
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.procs.Procs;
import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgProc;
import com.impossibl.postgres.system.tables.PgType;
import com.impossibl.postgres.types.Type.Category;
import com.impossibl.postgres.types.Type.Codec;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * Storage and loading for all the known types of a given context.
 *
 * @author kdubb
 *
 */
public class Registry {

  private static Logger logger = Logger.getLogger(Registry.class.getName());

  private Map<Character, Class<? extends Type>> kindMap;
  private TreeMap<Integer, Type> oidMap;
  private Map<String, Type> nameMap;
  private TreeMap<Integer, Type> relIdMap;

  private TreeMap<Integer, PgType.Row> pgTypeData;
  private TreeMap<Integer, Collection<PgAttribute.Row>> pgAttrData;
  private TreeMap<Integer, PgProc.Row> pgProcData;
  private Map<String, PgProc.Row> pgProcNameMap;

  private Context context;
  private Procs procs;
  private ReadWriteLock lock = new ReentrantReadWriteLock();

  private Map<String, String> typeNameAliases;


  public Registry(Context context) {

    this.context = context;

    this.procs = new Procs(context.getClass().getClassLoader());

    pgTypeData = new TreeMap<>();
    pgAttrData = new TreeMap<>();
    pgProcData = new TreeMap<>();
    pgProcNameMap = new HashMap<>();

    //Maps kinds to their associated type class
    kindMap = new HashMap<>();
    kindMap.put('c', CompositeType.class);
    kindMap.put('d', DomainType.class);
    kindMap.put('e', EnumerationType.class);
    kindMap.put('p', PsuedoType.class);
    kindMap.put('r', RangeType.class);

    // Required initial types for bootstrapping
    oidMap = new TreeMap<Integer, Type>();
    oidMap.put(16, new BaseType(16, "bool",     (short) 1,  (byte) 0, Category.Boolean, ',', 0, "bool", procs));
    oidMap.put(17, new BaseType(17, "bytea",    (short) 1,  (byte) 0, Category.User,    ',', 0, "bytea", procs));
    oidMap.put(18, new BaseType(18, "char",     (short) 1,  (byte) 0, Category.String,  ',', 0, "char", procs));
    oidMap.put(19, new BaseType(19, "name",     (short) 64, (byte) 0, Category.String,  ',', 0, "name", procs));
    oidMap.put(21, new BaseType(21, "int2",     (short) 2,  (byte) 0, Category.Numeric, ',', 0, "int2", procs));
    oidMap.put(23, new BaseType(23, "int4",     (short) 4,  (byte) 0, Category.Numeric, ',', 0, "int4", procs));
    oidMap.put(24, new BaseType(24, "regproc",  (short) 4,  (byte) 0, Category.Numeric, ',', 0, "regproc", procs));
    oidMap.put(25, new BaseType(25, "text",     (short) 1,  (byte) 0, Category.String,  ',', 0, "text", procs));
    oidMap.put(26, new BaseType(26, "oid",      (short) 4,  (byte) 0, Category.Numeric, ',', 0, "oid", procs));

    relIdMap = new TreeMap<>();
    nameMap = new HashMap<>();

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

          type = loadRaw(typeId);

          if (type == null) {

            context.refreshType(typeId);

          }

        }
        finally {
          lock.readLock().lock();
        }

        type = oidMap.get(typeId);

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

    boolean isArray = false;
    if (name .endsWith("[]")) {
      isArray = true;
      name = name.substring(0, name.length() - 2);
    }


    String alias = typeNameAliases.get(name);
    if (alias != null) {
      name = alias;
    }


    Type res;

    lock.readLock().lock();
    try {
      res = nameMap.get(name);
    }
    finally {
      lock.readLock().unlock();
    }

    if (res == null) {
      context.refreshType(getLatestKnownTypeId() + 1);
      lock.readLock().lock();
      try {
        res = nameMap.get(name);
      }
      finally {
        lock.readLock().unlock();
      }
    }

    if (isArray && res != null) {
      res = loadType(res.getArrayTypeId());
    }

    return res;
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

          type = loadRelationRaw(relationId);

          if (type == null) {

            context.refreshRelationType(relationId);

          }

        }
        finally {
          lock.readLock().lock();
        }

        type = (CompositeType) relIdMap.get(relationId);

      }

      return type;
    }
    finally {
      lock.readLock().unlock();
    }

  }

  public void unloadType(int oid) {
    Type type = oidMap.remove(oid);
    if (type != null) {
      nameMap.remove(type.getName());
    }
  }

  public void unloadType(String name) {
    Type type = nameMap.remove(name);
    if (type != null) {
      oidMap.remove(type.getId());
    }
  }

  /**
   * Looks up a procedures name given it's proc-id (aka OID)
   *
   * @param procId The procedure's id
   * @return The text name of the procedure or null, if none found
   */
  public String lookupProcName(int procId) {

    lock.readLock().lock();
    try {

      PgProc.Row pgProc = pgProcData.get(procId);
      if (pgProc == null)
        return null;

      return pgProc.name;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Looks up a procedure id (aka OID) given it's name.
   *
   * @param procName The procedure's name
   * @return The id of the procedure (aka OID) or null, if none found
   */
  public int lookupProcId(String procName) {

    lock.readLock().lock();
    try {

      PgProc.Row pgProc = pgProcNameMap.get(procName);
      if (pgProc == null)
        return 0;

      return pgProc.oid;
    }
    finally {
      lock.readLock().unlock();
    }

  }

  public int getLatestKnownTypeId() {
    return oidMap.lastKey();
  }

  /**
   * Updates the type information from the given catalog data. Any
   * information not specifically mentioned in the given updated
   * data will be retained and untouched.
   *
   * @param pgTypeRows "pg_type" table rows
   * @param pgAttrRows "pg_attribute" table rows
   * @param pgProcRows "pg_proc" table rows
   */
  public void update(Collection<PgType.Row> pgTypeRows, Collection<PgAttribute.Row> pgAttrRows, Collection<PgProc.Row> pgProcRows) {

    lock.writeLock().lock();
    try {
      /*
       * Update attribute info
       */

      //Remove attribute info for updating types
      for (PgType.Row pgType : pgTypeRows) {
        pgAttrData.remove(pgType.relationId);
      }

      //Add updated info
      for (PgAttribute.Row pgAttrRow : pgAttrRows) {

        Collection<PgAttribute.Row> relRows = pgAttrData.get(pgAttrRow.relationId);
        if (relRows == null) {
          relRows = new HashSet<PgAttribute.Row>();
          pgAttrData.put(pgAttrRow.relationId, relRows);
        }

        relRows.add(pgAttrRow);
      }

      /*
       * Update proc info
       */

      for (PgProc.Row pgProcRow : pgProcRows) {
        pgProcData.put(pgProcRow.oid, pgProcRow);
        pgProcNameMap.put(pgProcRow.name, pgProcRow);
      }


      /*
       * Update type info
       */
      for (PgType.Row pgTypeRow : pgTypeRows) {
        pgTypeData.put(pgTypeRow.oid, pgTypeRow);
        oidMap.remove(pgTypeRow.oid);
        nameMap.remove(pgTypeRow.name);
        relIdMap.remove(pgTypeRow.relationId);
      }

    }
    finally {
      lock.writeLock().unlock();
    }

    /*
     * (re)load all types just updated
     */
    for (PgType.Row pgType : pgTypeRows) {
      loadType(pgType.oid);
    }

  }

  /*
   * Materialize the requested type from the raw catalog data
   */
  private Type loadRaw(int typeId) {

    if (typeId == 0)
      return null;

    lock.writeLock().lock();
    try {

      PgType.Row pgType = pgTypeData.get(typeId);
      if (pgType == null)
        return null;

      Collection<PgAttribute.Row> pgAttrs = pgAttrData.get(pgType.relationId);

      Type type;

      lock.writeLock().unlock();
      try {
        type = loadRaw(pgType, pgAttrs);
      }
      finally {
        lock.writeLock().lock();
      }

      if (type != null) {
        oidMap.put(typeId, type);
        nameMap.put(type.getName(), type);
        relIdMap.put(type.getRelationId(), type);
      }

      return type;
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  /*
   * Materialize the requested type from the raw catalog data
   */
  private CompositeType loadRelationRaw(int relationId) {

    if (relationId == 0)
      return null;

    lock.writeLock().lock();
    try {

      CompositeType type = null;

      Collection<PgAttribute.Row> pgAttrs = pgAttrData.get(relationId);
      if (pgAttrs != null && !pgAttrs.isEmpty()) {

        PgType.Row pgType = pgTypeData.get(pgAttrs.iterator().next().relationTypeId);
        if (pgType == null)
          return null;

        lock.writeLock().unlock();
        try {
          type = (CompositeType) loadRaw(pgType, pgAttrs);
        }
        finally {
          lock.writeLock().lock();
        }

        if (type != null) {
          oidMap.put(pgType.oid, type);
          nameMap.put(type.getName(), type);
          relIdMap.put(type.getRelationId(), type);
        }
      }

      return type;
    }
    finally {
      lock.writeLock().unlock();
    }

  }

  /*
   * Materialize a type from the given "pg_type" and "pg_attribute" data
   */
  private Type loadRaw(PgType.Row pgType, Collection<PgAttribute.Row> pgAttrs) {

    Type type;

    if (pgType.elementTypeId != 0 && pgType.category.equals("A")) {

      ArrayType array = new ArrayType();
      array.setElementType(loadType(pgType.elementTypeId));

      type = array;
    }
    else {

      switch(pgType.discriminator.charAt(0)) {
        case 'b':
          type = new BaseType();
          break;
        case 'c':
          type = new CompositeType();
          break;
        case 'd':
          type = new DomainType();
          break;
        case 'e':
          type = new EnumerationType();
          break;
        case 'p':
          type = new PsuedoType();
          break;
        case 'r':
          type = new RangeType();
          break;
        default:
          logger.warning("unknown discriminator (aka 'typtype') found in pg_type table");
          return null;
      }

    }

    try {

      lock.writeLock().lock();
      try {
        oidMap.put(pgType.oid, type);
      }
      finally {
        lock.writeLock().unlock();
      }

      type.load(pgType, pgAttrs, this);

    }
    catch (Exception e) {

      e.printStackTrace();

      lock.writeLock().lock();
      try {
        oidMap.remove(pgType.oid);
      }
      finally {
        lock.writeLock().unlock();
      }
    }

    return type;
  }

  /**
   * Loads a matching Codec given the proc-id of its encoder and decoder
   *
   * @param encoderId proc-id of the encoder
   * @param decoderId proc-id of the decoder
   * @return A matching Codec instance
   */
  public Codec loadCodec(int encoderId, int decoderId, Format format) {

    Codec io = new Codec();
    io.decoder = loadDecoderProc(decoderId, procs.getDefaultDecoder(format));
    io.encoder = loadEncoderProc(encoderId, procs.getDefaultEncoder(format));
    return io;
  }

  /*
   * Loads a matching encoder given its proc-id
   */
  private Codec.Encoder loadEncoderProc(int procId, Codec.Encoder defaultEncoder) {

    String name = lookupProcName(procId);
    if (name == null) {
      return defaultEncoder;
    }

    return procs.loadEncoderProc(name, context, defaultEncoder);
  }

  /*
   * Loads a matching decoder given its proc-id
   */
  private Codec.Decoder loadDecoderProc(int procId, Codec.Decoder defaultDecoder) {

    String name = lookupProcName(procId);
    if (name == null) {
      return defaultDecoder;
    }

    return procs.loadDecoderProc(name, context, defaultDecoder);
  }

  /*
   * Loads a matching parser given mod-in and mod-out ids
   */
  public Modifiers.Parser loadModifierParser(int modInId, int modOutId) {

    String name = lookupProcName(modInId);
    if (name == null) {
      name = ""; //get the default proc
    }

    return procs.loadModifierParserProc(name, context);
  }

}
