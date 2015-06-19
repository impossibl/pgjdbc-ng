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
/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2014, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
package org.postgresql.util;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to tokenize the text output of org.postgres.
 * It's mainly used by the geometric classes, but is useful in parsing any
 * output from custom data types output from org.postgresql.
 *
 * @see org.postgresql.geometric.PGbox
 * @see org.postgresql.geometric.PGcircle
 * @see org.postgresql.geometric.PGlseg
 * @see org.postgresql.geometric.PGpath
 * @see org.postgresql.geometric.PGpoint
 * @see org.postgresql.geometric.PGpolygon
 */
public class PGtokenizer {
  // Our tokens
  protected List<String> tokens;

  /*
   * Create a tokeniser.
   * 
   * <p>We could have used StringTokenizer to do this, however, we needed to
   * handle nesting of '(' ')' '[' ']' '&lt;' and '&gt;' as these are used
   * by the geometric data types.
   * 
   * @param string containing tokens
   * 
   * @param delim single character to split the tokens
   */
  public PGtokenizer(String string, char delim) {
    tokenize(string, delim);
  }

  /*
   * This resets this tokenizer with a new string and/or delimiter.
   * 
   * @param string containing tokens
   * 
   * @param delim single character to split the tokens
   */
  public int tokenize(String string, char delim) {
    tokens = new ArrayList<>();

    // nest holds how many levels we are in the current token.
    // if this is > 0 then we don't split a token when delim is matched.
    //
    // The Geometric datatypes use this, because often a type may have others
    // (usualls PGpoint) imbedded within a token.
    //
    // Peter 1998 Jan 6 - Added < and > to the nesting rules
    int nest = 0, p, s;
    boolean skipChar = false;
    boolean nestedDoubleQuote = false;

    for (p = 0, s = 0; p < string.length(); p++) {
      char c = string.charAt(p);

      // increase nesting if an open character is found
      if (c == '(' || c == '[' || c == '<' || (!nestedDoubleQuote && !skipChar && c == '"')) {
        nest++;
        if (c == '"') {
          nestedDoubleQuote = true;
          skipChar = true;
        }
      }

      // decrease nesting if a close character is found
      if (c == ')' || c == ']' || c == '>' || (nestedDoubleQuote && !skipChar && c == '"')) {
        nest--;
        if (c == '"')
          nestedDoubleQuote = false;
      }

      if (c == '\\')
        skipChar = true;
      else
        skipChar = false;

      if (nest == 0 && c == delim) {
        tokens.add(string.substring(s, p));
        s = p + 1; // +1 to skip the delimiter
      }

    }

    // Don't forget the last token ;-)

    if (s < string.length())
      tokens.add(string.substring(s));

    return tokens.size();
  }

  /*
   * @return the number of tokens available
   */
  public int getSize() {
    return tokens.size();
  }

  /*
   * @param n Token number ( 0 ... getSize()-1 )
   * 
   * @return The token value
   */
  public String getToken(int n) {
    return tokens.get(n);
  }

  /*
   * This returns a new tokenizer based on one of our tokens.
   * 
   * The geometric datatypes use this to process nested tokens (usually
   * PGpoint).
   * 
   * @param n Token number ( 0 ... getSize()-1 )
   * 
   * @param delim The delimiter to use
   * 
   * @return A new instance of PGtokenizer based on the token
   */
  public PGtokenizer tokenizeToken(int n, char delim) {
    return new PGtokenizer(getToken(n), delim);
  }

  /*
   * This removes the lead/trailing strings from a string
   * 
   * @param s Source string
   * 
   * @param l Leading string to remove
   * 
   * @param t Trailing string to remove
   * 
   * @return String without the lead/trailing strings
   */
  public static String remove (String s, String l, String t) {
    if (s.startsWith(l))
      s = s.substring(l.length());
    if (s.endsWith(t))
      s = s.substring(0, s.length() - t.length());
    return s;
  }

  /*
   * This removes the lead/trailing strings from all tokens
   * 
   * @param l Leading string to remove
   * 
   * @param t Trailing string to remove
   */
  public void remove (String l, String t) {
    for (int i = 0; i < tokens.size(); i++) {
      tokens.set(i, remove (tokens.get(i), l, t));
    }
  }

  /*
   * Removes ( and ) from the beginning and end of a string
   * 
   * @param s String to remove from
   * 
   * @return String without the ( or )
   */
  public static String removePara(String s) {
    return remove(s, "(", ")");
  }

  /*
   * Removes ( and ) from the beginning and end of all tokens
   * 
   * @return String without the ( or )
   */
  public void removePara() {
    remove("(", ")");
  }

  /*
   * Removes [ and ] from the beginning and end of a string
   * 
   * @param s String to remove from
   * 
   * @return String without the [ or ]
   */
  public static String removeBox(String s) {
    return remove(s, "[", "]");
  }

  /*
   * Removes [ and ] from the beginning and end of all tokens
   * 
   * @return String without the [ or ]
   */
  public void removeBox() {
    remove("[", "]");
  }

  /*
   * Removes &lt; and &gt; from the beginning and end of a string
   * 
   * @param s String to remove from
   * 
   * @return String without the &lt; or &gt;
   */
  public static String removeAngle(String s) {
    return remove(s, "<", ">");
  }

  /*
   * Removes &lt; and &gt; from the beginning and end of all tokens
   * 
   * @return String without the &lt; or &gt;
   */
  public void removeAngle() {
    remove("<", ">");
  }
}
