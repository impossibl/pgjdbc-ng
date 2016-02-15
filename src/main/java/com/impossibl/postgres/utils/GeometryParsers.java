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

import com.impossibl.postgres.api.data.Path;

import java.util.Arrays;

public enum GeometryParsers {
  INSTANCE;

  /**
   * Parses a path.
   * <pre>
   * [ ( x1 , y1 ) , ... , ( xn , yn ) ]
   * ( ( x1 , y1 ) , ... , ( xn , yn ) )
   * ( x1 , y1 ) , ... , ( xn , yn )
   * ( x1 , y1   , ... ,   xn , yn )
   *  x1 , y1   , ... ,   xn , yn
   * </pre>
   * @param s The path to parse.
   * @return A Path.
   */
  public Path parsePath(CharSequence s) {
    return parsePath(s, true);
  }

  /**
   * Parses a polygon.
   * <pre>
   * ( ( x1 , y1 ) , ... , ( xn , yn ) )
   * ( x1 , y1 ) , ... , ( xn , yn )
   * ( x1 , y1   , ... ,   xn , yn )
   *  x1 , y1   , ... ,   xn , yn
   * </pre>
   * @param s The polygon to parse.
   * @return A list of points.
   */
  public double[][] parsePolygon(CharSequence s) {
    Path path = parsePath(s, false);
    return path.getPoints();
  }

  /**
   * Parses a circle.
   * <pre>
   * &lt; ( x , y ) , r &gt;
   * ( ( x , y ) , r )
   * ( x , y ) , r
   * x , y   , r
   * </pre>
   * @param s The circle to parse.
   * @return [p.x, p.y, r]: A 3 elements array where the first 2 doubles represents a center point and the last element the radius.
   */
  public double[] parseCircle(CharSequence s) {
    int max = s.length() - 1;
    int pos = consummeSpace(s, 0, true);
    boolean delim;
    char endDelim = ' ';
    int idx;
    if (s.charAt(pos) == '<') {
      ++pos;
      delim = true;
      endDelim = '>';
    }
    else if (s.charAt(pos) == '(') {
      // need to check next char
      idx = pos;
      pos = consummeSpace(s, ++pos, true);
      if (pos < max && s.charAt(pos) == '(') {
        delim = true;
        endDelim = ')';
        pos = idx + 1;
      }
      else {
        delim = false;
        pos = idx;
      }
    }
    else {
      delim = false;
    }
    double[] point = new double[2];
    pos = parsePoint(s, pos, point);
    pos = consummeSpace(s, pos, true);
    if (s.charAt(pos) != ',') {
      throw new IllegalArgumentException("near " + pos + " in  " + s + " - " + s.charAt(pos));
    }
    pos = consummeSpace(s, ++pos, true);
    double radius;
    idx = pos;
    if (delim) {
      while (pos <= max && s.charAt(pos) != endDelim) {
        ++pos;
      }
      if (idx == pos) {
        throw new IllegalArgumentException("near " + idx + " in  " + s);
      }
    }
    else {
      pos = parseNumber(s, pos);
    }
    radius = Double.parseDouble(s.subSequence(idx, pos).toString());
    pos = consummeSpace(s, pos, false);
    if (pos < max) {
      // too much chars
      throw new IllegalArgumentException("near " + pos + " in  " + s);
    }
    return new double[] {point[0], point[1], radius};
  }

  /**
   * Parses a point.
   * <pre>
   * ( x , y )
   *   x , y
   * </pre>
   * @param s The point to parse.
   * @return [p.x, p.y]
   */
  public double[] parsePoint(CharSequence s) {
    double[] point = new double[2];
    int pos = parsePoint(s, 0, point);
    pos = consummeSpace(s, pos, false);
    if (pos < s.length() - 1) {
      // too much chars
      throw new IllegalArgumentException("near " + pos + " in  " + s);
    }
    return point;
  }

  /**
   * Parses a box.
   * <pre>
   * ( ( x1 , y1 ) , ( x2 , y2 ) )
   * ( x1 , y1 ) , ( x2 , y2 )
   *  x1 , y1   ,   x2 , y2
   * </pre>
   * @param s The box to parse.
   * @return An array of size 4 (2 points.)
   */
  public double[] parseBox(CharSequence s) {
    PathResult pr = parsePath(false, 2, s, 0);
    int pos = consummeSpace(s, pr.pos, false);
    if (pos < s.length() - 1) {
      // too much chars
      throw new IllegalArgumentException("near " + pos + " in  " + s);
    }
    return new double[] {pr.p[0][0], pr.p[0][1], pr.p[1][0], pr.p[1][1]};
  }

  /**
   * Parses a lseg.
   * <pre>
   * [ ( x1 , y1 ) , ( x2 , y2 ) ]
   * ( ( x1 , y1 ) , ( x2 , y2 ) )
   * ( x1 , y1 ) , ( x2 , y2 )
   *  x1 , y1   ,   x2 , y2
   * </pre>
   * @param s The lseg to parse.
   * @return An array of size 4 (2 points.)
   */
  public double[] parseLSeg(CharSequence s) {
    PathResult pr = parsePath(true, 2, s, 0);
    int pos = consummeSpace(s, pr.pos, false);
    if (pos < s.length() - 1) {
      // too much chars
      throw new IllegalArgumentException("near " + pos + " in  " + s);
    }
    return new double[] {pr.p[0][0], pr.p[0][1], pr.p[1][0], pr.p[1][1]};
  }

  /**
   * Parses an infinite line represented by the linear equation Ax + By + C = 0.
   * <pre>
   * { A, B, C }
   * also accepted:
   * [ ( x1 , y1 ) , ( x2 , y2 ) ]
   * ( ( x1 , y1 ) , ( x2 , y2 ) )
   * ( x1 , y1 ) , ( x2 , y2 )
   *  x1 , y1   ,   x2 , y2
   * </pre>
   * @param s The line to parse.
   * @return An array of size 3 ([A,B,C] --&gt; Ax+By+C=0.)
   */
  public double[] parseLine(CharSequence s) {
    int pos = consummeSpace(s, 0, true);
    double[] result;
    if (s.charAt(pos) == '{') {
      pos = parseLineABC(s, pos, result = new double[3]);
    }
    else {
      PathResult pr = parsePath(true, 2, s, pos);
      if (Arrays.equals(pr.p[0], pr.p[1])) {
        throw new IllegalArgumentException("invalid line specification: must be two distinct points");
      }
      pos = pr.pos;
      result = lineConstructPts(pr.p[0], pr.p[1]);
    }
    pos = consummeSpace(s, pos, false);
    if (pos < s.length() - 1) {
      // too much chars
      throw new IllegalArgumentException("near " + pos + " in  " + s);
    }
    return result;
  }

  /**
   * Returns A,B and C as in Ax+By+C=0 given 2 points
   */
  private static double[] lineConstructPts(double[] pt1, double[] pt2) {
    double[] lineabc = new double[3];
    if (pt1[0] == pt2[0]) { /* vertical */
      /* use "x = C" */
      lineabc[0] = -1;
      lineabc[1] = 0;
      lineabc[2] = pt1[0];
    }
    else if (pt1[1] == pt2[1]) { /* horizontal */
      /* use "y = C" */
      lineabc[0] = 0;
      lineabc[1] = -1;
      lineabc[2] = pt1[1];
    }
    else {
      /* use "mx - y + yinter = 0" */
      lineabc[0] = (pt2[1] - pt1[1]) / (pt2[0] - pt1[0]);
      lineabc[1] = -1.0;
      lineabc[2] = pt1[1] - lineabc[0] * pt1[0];
    }
    return lineabc;
  }

  private int parseLineABC(CharSequence s, int pos, double[] abc) {
    // A,B,C --> Ax+By+C=0
    if (s.charAt(pos) != '{') {
      throw new IllegalArgumentException("near " + pos + " in  " + s);
    }
    int pos1 = ++pos;
    pos = parseNumber(s, pos);
    double a = Double.parseDouble(s.subSequence(pos1, pos).toString());

    pos = consummeSpace(s, pos, true);
    if (s.charAt(pos) != ',') {
      throw new IllegalArgumentException("near " + pos + " in  " + s);
    }
    pos = consummeSpace(s, ++pos, true);

    pos1 = pos;
    pos = parseNumber(s, pos);
    double b = Double.parseDouble(s.subSequence(pos1, pos).toString());
    if (a == 0 && b == 0) {
      throw new IllegalArgumentException("invalid line specification: A and B cannot both be zero");
    }
    pos = consummeSpace(s, pos, true);
    if (s.charAt(pos) != ',') {
      throw new IllegalArgumentException("near " + pos + " in  " + s);
    }
    pos = consummeSpace(s, ++pos, true);

    pos1 = pos;
    pos = parseNumber(s, pos);
    double c = Double.parseDouble(s.subSequence(pos1, pos).toString());

    pos = consummeSpace(s, pos, true);
    if (s.charAt(pos) != '}') {
      throw new IllegalArgumentException("near " + pos + " in  " + s);
    }
    ++pos;
    abc[0] = a;
    abc[1] = b;
    abc[2] = c;
    return pos;
  }

  private Path parsePath(CharSequence s, boolean acceptopen) {
    int npts = pairCount(s, ',');
    if (npts < 0) {
      throw new IllegalArgumentException("invalid input syntax for type path: " + s);
    }
    int pos = consummeSpace(s, 0, true);
    int max = s.length() - 1;
    int depth = 0;
    if (s.charAt(pos) == '(' && findLastDelim(s, pos, '(') == pos + 1) {
      ++pos;
      ++depth;
    }
    PathResult pr = parsePath(acceptopen, npts, s, consummeSpace(s, pos, true));
    if (depth != 0) {
      pr.pos = consummeSpace(s, pr.pos, true);
      if (s.charAt(pr.pos) != ')') {
        throw new IllegalArgumentException("near " + pr.pos + " in  " + s);
      }
      ++pos;
    }
    pr.pos = consummeSpace(s, pr.pos, false);
    if (pr.pos < max) {
      // too much chars
      throw new IllegalArgumentException("near " + pr.pos + " in  " + s);
    }
    return new Path(pr.p, !pr.isOpen);
  }

  private static int pairCount(CharSequence s, char delim) {
    int ndelim = 0;
    int max = s.length() - 1;
    int pos = 0;
    while (pos < max) {
      if (s.charAt(pos) == delim) {
        ++ndelim;
      }
      ++pos;
    }
    return (ndelim % 2) == 0 ? -1 : ((ndelim + 1) / 2);
  }

  public static void main(String... args) {
    System.out.println("Circles ===========");
    String[] circles = new String[] {
      "124.66, -565.88, 1256.6",
      "(124.66, -565.88), 1256.6",
      "((124.66, -565.88), 1256.6)",
      "<(124.66, -565.88), 1256.6>"
    };
    for (String c: circles) {
      System.out.println(c + " --> " + java.util.Arrays.toString(GeometryParsers.INSTANCE.parseCircle(c)));
    }
    System.out.println("Numbers ===========");
    String[] numbers = new String[] {
      "99.4", "89E03", "8e-3", "45.3E+45"
    };
    for (String n: numbers) {
      int pos = GeometryParsers.INSTANCE.parseNumber(n, 0);
      System.out.println(n + " " + pos + " " + n.length() + " " + n.substring(0, pos));
    }
    System.out.println("Points ===========");
    String[] points = new String[] {
      "(100, 100)",
      "(-343.43, 43.01)",
      "100.0, 99.4",
      "(0,0)", "(124, 24343)", "(-113.7, +8989.98)",
      "(100e+2, 100e-2)",
      "(-343.43, 43.01)",
      "100.0e4, 99.4e-2",
    };
    for (String p: points) {
      try {
        double[] point = GeometryParsers.INSTANCE.parsePoint(p);
        System.out.println(p + " --> " + java.util.Arrays.toString(point));
      }
      catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    System.out.println("Boxes ===========");
    String[] boxes = new String[] {
      "((678.6,454),(124.6,0))",
      "(678.6,454),(124.6,0)",
      "(0,0),(0,0)",
      "(-678.6, -454),(124.6,1.0)",
      "-678.6, -454,124.6,1.0",
      "(10, 20),(100, 120)"
    };
    for (String box: boxes) {
      try {
        double[] b = GeometryParsers.INSTANCE.parseBox(box);
        System.out.println(box + " --> " + java.util.Arrays.toString(b));
      }
      catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    System.out.println("LSegs ===========");
    String[] lsegs = new String[] {
      "[(678.6,454),(124.6,0)]",
      "((678.6,454),(124.6,0))",
      "(678.6,454),(124.6,0)",
      "(0,0),(0,0)",
      "(-678.6, -454),(124.6,1.0)",
      "-678.6, -454,124.6,1.0",
      "(10, 20),(100, 120)"
    };
    for (String lseg: lsegs) {
      try {
        double[] b = GeometryParsers.INSTANCE.parseLSeg(lseg);
        System.out.println(lseg + " --> " + java.util.Arrays.toString(b));
      }
      catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    System.out.println("Paths ===========");
    String[] paths = new String[] {
      "[(678.6,454)]",
      "((678.6,454))",
      "[(678.6,454),(124.6,0)]",
      "((678.6,454),(124.6,0))",
      "(678.6,454),(124.6,0)",
      "(0,0),(0,0)",
      "(-678.6, -454),(124.6,1.0)",
      "-678.6, -454,124.6,1.0",
      "(10, 20),(100, 120)",
      "[(678.6,454),(10,89),(124.6,0)]",
      "((678.6,454),(10,89),(124.6,0))",
      "(678.6,454),(10,89),(124.6,0)",
      "(0,0),(10,89),(0,0)",
      "(-678.6, -454),(10,89),(124.6,1.0)",
      "-678.6,10,89, -454,124.6,1.0",
      "(10, 20),(10,89),(100, 120)"
    };
    for (String path: paths) {
      try {
        Path pr = GeometryParsers.INSTANCE.parsePath(path);
        System.out.println(path + " --> " + pr);
      }
      catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }

  private static int findLastDelim(CharSequence s, int pos, char delim) {
    int found = -1;
    int max = s.length() - 1;
    while (pos < max) {
      if (s.charAt(pos) == delim) {
        found = pos;
      }
      ++pos;
    }
    return found;
  }

  private PathResult parsePath(boolean opendelim, int npts, CharSequence s, int pos) {
    PathResult pr = new PathResult();
    int depth = 0;
    pr.pos = consummeSpace(s, pos, true);
    char c = s.charAt(pr.pos);
    if (c == '[') {
      pr.isOpen = true;
      if (!opendelim) {
        throw new IllegalArgumentException("near " + pr.pos + " in  " + s);
      }
      ++pr.pos;
      ++depth;
      pr.pos = consummeSpace(s, pr.pos, true);
    }
    else if (c == '(') {
      int cp = consummeSpace(s, pr.pos + 1, true);
      if (s.charAt(cp) == '(') {
        /* nested delimiters with only one point? */
        if (npts <= 1) {
          throw new IllegalArgumentException("near " + pr.pos + " in  " + s);
        }
        depth++;
        pr.pos = cp;
      }
      else {
        if (findLastDelim(s, pr.pos, '(') == cp) {
          depth++;
          pr.pos = cp;
        }
      }
    }
    pr.p = new double[npts][];
    int max = s.length() - 1;
    for (int i = 0; i < npts; i++) {
      double[] points = new double[2];
      pr.pos = parsePoint(s, pr.pos, points);
      pr.pos = consummeSpace(s, pr.pos, i < npts - 1);
      if (pr.pos < max && s.charAt(pr.pos) == ',') {
        ++pr.pos;
      }
      pr.p[i] = points;
    }
    while (depth > 0) {
      if ((s.charAt(pr.pos) == ')') || ((s.charAt(pr.pos) == ']') && pr.isOpen && (depth == 1))) {
        depth--;
        pr.pos++;
        pr.pos = consummeSpace(s, pr.pos, depth != 0);
      }
      else {
        throw new IllegalArgumentException("near " + pr.pos + " in  " + s);
      }
    }
    return pr;
  }

  private int parsePoint(CharSequence s, int pos, double[] p) {
    int max = s.length() - 1;
    pos = consummeSpace(s, pos, true);
    boolean delim = false;
    if ('(' == s.charAt(pos)) {
      ++pos;
      delim = true;
    }
    pos = consummeSpace(s, pos, true);
    int pos1 = pos;
    while (pos <= max && ',' != s.charAt(pos)) {
      ++pos;
    }
    double p1 = Double.parseDouble(s.subSequence(pos1, pos).toString());
    pos = consummeSpace(s, ++pos, true);
    double p2;
    pos1 = pos;
    if (delim) {
      while (pos <= max && ')' != s.charAt(pos)) {
        ++pos;
      }
      p2 = Double.parseDouble(s.subSequence(pos1, pos).toString());
    }
    else {
      pos = parseNumber(s, pos);
      p2 = Double.parseDouble(s.subSequence(pos1, pos).toString());
    }
    pos = consummeSpace(s, pos, delim);
    if (delim) {
      if (s.charAt(pos) != ')') {
        throw new IllegalArgumentException("near " + pos + " in  " + s);
      }
      ++pos;
    }
    p[0] = p1;
    p[1] = p2;
    return pos;
  }

  private static int parseExponent(CharSequence s, int pos) {
    int max = s.length() - 1;
    if (pos >= max) {
      return pos;
    }
    char c = s.charAt(pos);
    if (c == 'e' || c == 'E') {
      ++pos;
      if (pos >= max) {
        throw new NumberFormatException("near " + pos + " in  " + s);
      }
      c = s.charAt(pos);
      if (c == '-' || c == '+') {
        ++pos;
      }
      if (pos > max) {
        throw new NumberFormatException("near " + pos + " in  " + s);
      }
      int initpos = pos;
      while (pos <= max && Character.isDigit(s.charAt(pos))) {
        ++pos;
      }
      if (initpos == pos) { // at least one is needed
        throw new NumberFormatException("near " + pos + " in  " + s);
      }
    }
    return pos;
  }

  private int parseNumber(CharSequence s, int pos) {
    int max = s.length() - 1;
    boolean dot = false;
    switch (s.charAt(pos)) {
      case '.':
      case '-':
      case '+':
      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
      case '8':
      case '9':
        // looks like a number
        // eat a number
        dot = (s.charAt(pos) == '.');
        ++pos;
        while (pos <= max) {
          char c = s.charAt(pos);
          if ('.' == c) {
            if (dot) { // two dots
              throw new NumberFormatException("near " + pos + " in  " + s);
            }
            dot = true;
          }
          else if ((!(c >= '0' && c <= '9')) && !Character.isDigit(c)) {
            break;
          }
          pos++;
        }
        if (pos <= max) {
          pos = parseExponent(s, pos);
        }
    }
    return pos;
  }

  private static int consummeSpace(CharSequence s, int pos, boolean checkEOS) {
    int max = s.length() - 1;
    while (pos <= max && Character.isSpaceChar(s.charAt(pos))) {
      ++pos;
    }

    if (checkEOS && pos > max) {
      throw new IllegalArgumentException("near " + pos + " in  " + s);
    }
    return pos;
  }

  static class PathResult {
    int pos;
    double[][] p;
    boolean isOpen = false;
  }
}
