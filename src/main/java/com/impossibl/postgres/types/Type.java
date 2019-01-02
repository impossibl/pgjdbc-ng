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
import com.impossibl.postgres.protocol.TypeRef;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.tables.PGTypeTable;

import static com.impossibl.postgres.system.Settings.FIELD_FORMAT_PREF;
import static com.impossibl.postgres.system.Settings.FIELD_FORMAT_PREF_DEFAULT;
import static com.impossibl.postgres.system.Settings.PARAM_FORMAT_PREF;
import static com.impossibl.postgres.system.Settings.PARAM_FORMAT_PREF_DEFAULT;
import static com.impossibl.postgres.system.Settings.getSystemProperty;
import static com.impossibl.postgres.utils.guava.Preconditions.checkNotNull;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

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
public abstract class Type implements TypeRef {

  public static final String CATALOG_NAMESPACE = "pg_catalog";
  public static final String PUBLIC_NAMESPACE = "public";

  public enum Category {
    Array('A'),
    Boolean('B'),
    Composite('C'),
    DateTime('D'),
    Enumeration('E'),
    Geometry('G'),
    NetworkAddress('I'),
    Numeric('N'),
    Psuedo('P'),
    Range('R'),
    String('S'),
    Timespan('T'),
    User('U'),
    BitString('V'),
    Unknown('X');

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
     * @param id Category id
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
  public static class Codec<InBuffer, OutBuffer> {

    /**
     *  Decodes the given data into a Java language object
     */
    public interface Decoder<InBuffer> {

      PrimitiveType getPrimitiveType();

      Class<?> getDefaultClass();

      Object decode(Context context, Type type, Short typeLength, Integer typeModifier, InBuffer buffer, Class<?> targetClass, Object targetContext) throws IOException;

    }

    /**
     * Encodes the given Java language object as data the server expects.
     */
    public interface Encoder<OutBuffer> {

      PrimitiveType getPrimitiveType();

      void encode(Context context, Type type, Object value, Object sourceContext, OutBuffer buffer) throws IOException;

    }

    private Decoder<InBuffer> decoder;
    private Encoder<OutBuffer> encoder;

    public Codec(Decoder<InBuffer> decoder, Encoder<OutBuffer> encoder) {
      this.decoder = decoder;
      this.encoder = encoder;
    }

    /**
     * Get the encoder
     * @return The value
     */
    public Encoder<OutBuffer> getEncoder() {
      return encoder;
    }

    /**
     * Get the decoder
     * @return The value
     */
    public Decoder<InBuffer> getDecoder() {
      return decoder;
    }
  }

  public static class BinaryCodec extends Codec<ByteBuf, ByteBuf> {
    public BinaryCodec(Decoder<ByteBuf> decoder, Encoder<ByteBuf> encoder) {
      super(decoder, encoder);
    }
  }

  public static class TextCodec extends Codec<CharSequence, StringBuilder> {
    public TextCodec(Decoder<CharSequence> decoder, Encoder<StringBuilder> encoder) {
      super(decoder, encoder);
    }
  }

  private int id;
  private QualifiedName name;
  private Short length;
  private Byte alignment;
  private Category category;
  private Character delimeter;
  private int arrayTypeId;
  private int relationId;
  private TextCodec textCodec;
  private BinaryCodec binaryCodec;
  private Modifiers.Parser modifierParser;
  private FieldFormat preferredParameterFormat;
  private FieldFormat preferredResultFormat;

  public Type() {
  }

  public Type(int id, String name, String namespace, Short length, Byte alignment, Category category, Character delimeter, Integer arrayTypeId, BinaryCodec binaryCodec, TextCodec textCodec, Modifiers.Parser modifierParser, FieldFormat preferredParameterFormat, FieldFormat preferredResultFormat) {
    super();
    this.id = id;
    this.name = new QualifiedName(namespace, name);
    this.length = length;
    this.alignment = alignment;
    this.category = checkNotNull(category);
    this.delimeter = delimeter;
    this.arrayTypeId = arrayTypeId;
    this.binaryCodec = binaryCodec;
    this.textCodec = textCodec;
    this.modifierParser = modifierParser;
    this.preferredParameterFormat = preferredParameterFormat;
    this.preferredResultFormat = preferredResultFormat;
  }

  @Override
  public int getOid() {
    return id;
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name.getLocalName();
  }

  public String getNamespace() {
    return name.getNamespace();
  }

  public QualifiedName getQualifiedName() {
    return name;
  }

  public Short getLength() {
    return length;
  }

  public Byte getAlignment() {
    return alignment;
  }

  public Category getCategory() {
    return category;
  }

  public char getDelimeter() {
    return delimeter;
  }

  public int getArrayTypeId() {
    return arrayTypeId;
  }

  public Boolean isNullable() {
    return null;
  }

  public String getDefaultValue() {
    return null;
  }

  public boolean isAutoIncrement() {
    return isAutoIncrement(getDefaultValue());
  }

  public static boolean isAutoIncrement(String defaultValue) {
    return defaultValue != null && defaultValue.startsWith("nextval(");
  }

  public BinaryCodec getBinaryCodec() {
    return binaryCodec;
  }

  public TextCodec getTextCodec() {
    return textCodec;
  }

  public Codec<?, ?> getCodec(FieldFormat format) {
    switch (format) {
      case Text: return textCodec;
      case Binary: return binaryCodec;
      default:
        throw new IllegalArgumentException();
    }
  }

  public Modifiers.Parser getModifierParser() {
    return modifierParser;
  }

  public int getRelationId() {
    return relationId;
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
    if (binCodec.decoder.getPrimitiveType() != null) {
      return binCodec.decoder.getPrimitiveType();
    }
    Codec txtCodec = getTextCodec();
    if (txtCodec.decoder.getPrimitiveType() != null) {
      return txtCodec.decoder.getPrimitiveType();
    }
    return PrimitiveType.Unknown;
  }

  public boolean isParameterFormatSupported(FieldFormat format) {
    return getCodec(format).encoder.getPrimitiveType() != PrimitiveType.Unknown;
  }

  public FieldFormat getParameterFormat() {

    if (category == Category.String) {
      return FieldFormat.Text;
    }

    if (isParameterFormatSupported(preferredParameterFormat))
      return preferredParameterFormat;

    FieldFormat other = preferredParameterFormat == FieldFormat.Binary ? FieldFormat.Text : FieldFormat.Binary;

    if (isParameterFormatSupported(other))
      return other;

    throw new IllegalStateException("type has no supported parameter format: " + toString());
  }

  public boolean isResultFormatSupported(FieldFormat format) {
    return getCodec(format).decoder.getPrimitiveType() != PrimitiveType.Unknown;
  }

  public FieldFormat getResultFormat() {

    if (category == Category.String) {
      return FieldFormat.Text;
    }

    if (isResultFormatSupported(preferredResultFormat))
      return preferredResultFormat;

    return preferredResultFormat == FieldFormat.Binary ? FieldFormat.Text : FieldFormat.Binary;
  }

  /**
   * Load this type from a "pg_type" table row.
   *
   * @param source The "pg_type" table entry
   * @param registry The registry that is loading the type.
   */
  public void load(PGTypeTable.Row source, Registry registry) {
    id = source.getOid();
    name = new QualifiedName(source.getNamespace(), source.getName());
    length = source.getLength() != -1 ? source.getLength() : null;
    alignment = getAlignment(source.getAlignment() != null ? source.getAlignment().charAt(0) : null);
    category = Category.findValue(source.getCategory());
    delimeter = source.getDeliminator() != null ? source.getDeliminator().charAt(0) : null;
    arrayTypeId = source.getArrayTypeId();
    relationId = source.getRelationId();
    textCodec = registry.getShared().loadTextCodec(source.getInputId(), source.getOutputId());
    binaryCodec = registry.getShared().loadBinaryCodec(source.getReceiveId(), source.getSendId());
    modifierParser = registry.getShared().loadModifierParser(source.getModInId());
    preferredParameterFormat = FieldFormat.valueOf(getSystemProperty(PARAM_FORMAT_PREF, PARAM_FORMAT_PREF_DEFAULT));
    preferredResultFormat = FieldFormat.valueOf(getSystemProperty(FIELD_FORMAT_PREF, FIELD_FORMAT_PREF_DEFAULT));
  }

  /**
   * Translates a protocol alignment id into a specific number of bytes.
   *
   * @param align Alignment ID
   * @return # of bytes to align on
   */
  private static Byte getAlignment(Character align) {

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
    return name.toString() + '(' + id + ')';
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
    return id == other.id;
  }

}
