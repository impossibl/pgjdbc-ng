/*
 * Copyright 2019, OnGres.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package com.impossibl.postgres.protocol.sasl;

import java.nio.CharBuffer;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;

public class SaslPrep {

  private static final int MAX_UTF = 65535;

  /**
   * SaslPrep profile of StringPrep algorithm.
   * @param value with the value to check and transform
   * @param storedString boolean to indicate if it's a stored string or a query string
   * @return String with the result
   */
  public static String saslPrep(String value, boolean storedString) {
    List<Integer> valueBuilder = new ArrayList<>();
    //Mapping
    //non-ASCII space characters
    List<Integer> codePoints = new ArrayList<>();
    for (int i = 0; i < value.length(); i++) {
      int codePoint = value.codePointAt(i);
      codePoints.add(codePoint);
      if (codePoint > MAX_UTF) {
        i++;
      }
      if (!StringPrep.prohibitionNonAsciiSpace(codePoint)) {
        valueBuilder.add(codePoint);
      }
    }
    //commonly mapped to nothing
    StringBuilder stringBuilder = new StringBuilder();

    for (int codePoint : codePoints) {
      if (!StringPrep.mapToNothing(codePoint)) {
        char[] characters = Character.toChars(codePoint);
        stringBuilder.append(characters);
      }
    }

    //Normalization
    String normalized = Normalizer.normalize(
        CharBuffer.wrap(stringBuilder.toString().toCharArray()), Normalizer.Form.NFKC);

    valueBuilder = new ArrayList<>();
    for (int i = 0; i < normalized.length(); i++) {
      int codePoint = normalized.codePointAt(i);
      codePoints.add(codePoint);
      if (codePoint > MAX_UTF) {
        i++;
      }
      if (!StringPrep.prohibitionNonAsciiSpace(codePoint)) {
        valueBuilder.add(codePoint);
      }
    }

    //Prohibited
    //Non-ASCII space characters
    //ASCII control characters
    //Non-ASCII control characters
    //Private Use characters
    //Non-character code points
    //Surrogate code points
    //Inappropriate for plain text characters
    //Inappropriate for canonical representation characters
    //Change display properties or deprecated characters
    //Tagging characters [StringPrep, C.9]
    for (int character : valueBuilder) {
      if (StringPrep.prohibitionNonAsciiSpace(character) ||
          StringPrep.prohibitionAsciiControl(character) ||
          StringPrep.prohibitionNonAsciiControl(character) ||
          StringPrep.prohibitionPrivateUse(character) ||
          StringPrep.prohibitionNonCharacterCodePoints(character) ||
          StringPrep.prohibitionSurrogateCodes(character) ||
          StringPrep.prohibitionInappropriatePlainText(character) ||
          StringPrep.prohibitionInappropriateCanonicalRepresentation(character) ||
          StringPrep.prohibitionChangeDisplayProperties(character) ||
          StringPrep.prohibitionTaggingCharacters(character)) {
        throw new IllegalArgumentException("Prohibited character " + String.valueOf(Character.toChars(character)));
      }
      //Unassigned Code Points
      if (storedString && StringPrep.unassignedCodePoints(character)) {
        throw new IllegalArgumentException("Prohibited character " + String.valueOf(Character.toChars(character)));
      }
    }
    //Bidirectional
    StringPrep.bidirectional(valueBuilder);

    return normalized;
  }
}
