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
import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgType;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 *
 * Represents a single type in the databases known types table. Type is the
 * base of a hierarchy that mirrors the kinds of types they represent.
 *
 *
 * NOTE: A Type, or one of its derived types, represents a single entry in
 * the "pg_type" table.
 *
 * @author kdubb
 *
 */
public abstract class Type {

  public enum Category {
    Array           ('A'),
    Boolean         ('B'),
    Composite       ('C'),
    DateTime        ('D'),
    Enumeration     ('E'),
    Geometry        ('G'),
    NetworkAddress  ('I'),
    Numeric         ('N'),
    Psuedo          ('P'),
    Range           ('R'),
    String          ('S'),
    Timespan        ('T'),
    User            ('U'),
    BitString       ('V'),
    Unknown         ('X');

    private char id;

    Category(char id) {
      this.id = id;
    }

    public char getId() {
      return id;
    }

    /**
     * Lookup Category by its associated "id".
     *
     * @param id
     * @return Associated category or null if none
     */
    public static Category findValue(String id) {

      if (id == null || id.isEmpty())
        return null;

      for (Category cat : values()) {
        if (cat.id == id.charAt(0))
          return cat;
      }

      return null;
    }

  }

  /**
   * A pair of related interface methods to encode/decode a type in a
   * specific format.  The are mapped to their equivalent procedures
   * in the database.
   */
  public static class Codec {

    /**
     *  Decodes the given data into a Java language object
     */
    public interface Decoder {
      PrimitiveType getInputPrimitiveType();
      Class<?> getOutputType();
      Object decode(Type type, Short typeLength, Integer typeModifier, Object buffer, Context context) throws IOException;
    }

    /**
     * Encodes the given Java language as data the server expects.
     */
    public interface Encoder {
      Class<?> getInputType();
      PrimitiveType getOutputPrimitiveType();
      void encode(Type type, Object buffer, Object value, Context context) throws IOException;
    }

    private Decoder decoder;
    private Encoder encoder;

    /**
     * Set the encoder
     * @param v The value
     */
    public void setEncoder(Encoder v) {
      encoder = v;
    }

    /**
     * Get the encoder
     * @return The value
     */
    public Encoder getEncoder() {
      return encoder;
    }

    /**
     * Set the decoder
     * @param v The value
     */
    public void setDecoder(Decoder v) {
      decoder = v;
    }

    /**
     * Get the decoder
     * @return The value
     */
    public Decoder getDecoder() {
      return decoder;
    }
  }

  private int id;
  private String name;
  private String namespace;
  private Short length;
  private Byte alignment;
  private Category category;
  private Character delimeter;
  private int arrayTypeId;
  private int relationId;
  private Codec[] codecs;
  private Modifiers.Parser modifierParser;

  public Type() {
  }

  public Type(int id, String name, Short length, Byte alignment, Category category, char delimeter, int arrayTypeId, Codec binaryCodec, Codec textCodec) {
    super();
    this.id = id;
    this.name = name;
    this.length = length;
    this.alignment = alignment;
    this.category = category;
    this.delimeter = delimeter;
    this.arrayTypeId = arrayTypeId;
    this.codecs = new Codec[]{textCodec, binaryCodec};
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public Short getLength() {
    return length;
  }

  public void setLength(Short length) {
    this.length = length;
  }

  public Byte getAlignment() {
    return alignment;
  }

  public void setAlignment(Byte alignment) {
    this.alignment = alignment;
  }

  public Category getCategory() {
    return category;
  }

  public void setCategory(Category category) {
    this.category = category;
  }

  public char getDelimeter() {
    return delimeter;
  }

  public void setDelimeter(char delimeter) {
    this.delimeter = delimeter;
  }

  public int getArrayTypeId() {
    return arrayTypeId;
  }

  public void setArrayTypeId(int arrayTypeId) {
    this.arrayTypeId = arrayTypeId;
  }

  public Codec getBinaryCodec() {
    return codecs[Format.Binary.ordinal()];
  }

  public void setBinaryCodec(Codec binaryCodec) {
    this.codecs[Format.Binary.ordinal()] = binaryCodec;
  }

  public Codec getTextCodec() {
    return codecs[Format.Text.ordinal()];
  }

  public void setTextCodec(Codec textCodec) {
    codecs[Format.Text.ordinal()] = textCodec;
  }

  public Codec getCodec(Format format) {
    return codecs[format.ordinal()];
  }

  public Modifiers.Parser getModifierParser() {
    return modifierParser;
  }

  public void setModifierParser(Modifiers.Parser modifierParser) {
    this.modifierParser = modifierParser;
  }

  public int getRelationId() {
    return relationId;
  }

  public void setRelationId(int relationId) {
    this.relationId = relationId;
  }

  /**
   * Strips all "wrapping" type (e.g. arrays, domains) and returns
   * the base type
   *
   * @return Base type after all unwrapping
   */
  public Type unwrap() {
    return this;
  }

  public PrimitiveType getPrimitiveType() {
    Codec binCodec = getBinaryCodec();
    if (binCodec.decoder.getInputPrimitiveType() != null) {
      return binCodec.decoder.getInputPrimitiveType();
    }
    Codec txtCodec = getTextCodec();
    if (txtCodec.decoder.getInputPrimitiveType() != null) {
      return txtCodec.decoder.getInputPrimitiveType();
    }
    return PrimitiveType.Unknown;
  }

  public Class<?> getJavaType(Format format, Map<String, Class<?>> customizations) {
    Codec codec = getCodec(format);
    if (codec.decoder.getInputPrimitiveType() != PrimitiveType.Unknown) {
      return codec.decoder.getOutputType();
    }
    return String.class;
  }

  public Format getPreferredFormat() {
    if (isParameterFormatSupported(Format.Binary) && isResultFormatSupported(Format.Binary))
      return Format.Binary;
    return Format.Text;
  }

  public boolean isParameterFormatSupported(Format format) {
    return getCodec(format).getEncoder().getOutputPrimitiveType() != PrimitiveType.Unknown;
  }

  public Format getParameterFormat() {

    if (isParameterFormatSupported(Format.Binary))
      return Format.Binary;

    if (isParameterFormatSupported(Format.Text))
      return Format.Text;

    throw new IllegalStateException("type has no supported parameter format: " + toString());
  }

  public boolean isResultFormatSupported(Format format) {
    return getCodec(format).decoder.getInputPrimitiveType() != PrimitiveType.Unknown;
  }

  public Format getResultFormat() {

    if (isResultFormatSupported(Format.Binary))
      return Format.Binary;

    return Format.Text;
  }

  /**
   * Load this type from a "pg_type" table entry and, if available, a
   * collection of "pg_attribute" table entries.
   *
   * @param source The "pg_type" table entry
   * @param attrs Associated "pg_attribute" table entries, if available.
   * @param registry The registry that is loading the type.
   */
  public void load(PgType.Row source, Collection<PgAttribute.Row> attrs, Registry registry) {

    id = source.getOid();
    name = source.getName();
    namespace = source.getNamespace();
    length = source.getLength() != -1 ? source.getLength() : null;
    alignment = getAlignment(source.getAlignment() != null ? source.getAlignment().charAt(0) : null);
    category = Category.findValue(source.getCategory());
    delimeter = source.getDeliminator() != null ? source.getDeliminator().charAt(0) : null;
    arrayTypeId = source.getArrayTypeId();
    relationId = source.getRelationId();
    codecs = new Codec[] {
      registry.loadCodec(source.getInputId(), source.getOutputId(), Format.Text),
      registry.loadCodec(source.getReceiveId(), source.getSendId(), Format.Binary),
    };
    modifierParser = registry.loadModifierParser(source.getModInId(), source.getModOutId());
  }

  /**
   * Translates a protocol alignment id into a specific number of bytes.
   *
   * @param align Alignment ID
   * @return # of bytes to align on
   */
  public static Byte getAlignment(Character align) {

    if (align == null)
      return null;

    switch (align) {
      case 'c':
        return 1;
      case 's':
        return 2;
      case 'i':
        return 4;
      case 'd':
        return 8;
    }

    throw new IllegalStateException("invalid alignment character: " + align);
  }

  @Override
  public String toString() {
    return name + '(' + id + ')';
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + id;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Type other = (Type) obj;
    if (id != other.id)
      return false;
    return true;
  }

}
