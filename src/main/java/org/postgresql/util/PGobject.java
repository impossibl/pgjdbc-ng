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

import java.io.Serializable;
import java.sql.SQLException;

/**
 * PGobject is a class used to describe unknown types
 * An unknown type is any type that is unknown by JDBC Standards
 */
public class PGobject implements Serializable, Cloneable {
  protected String type;
  protected String value;

  /**
   * This is called by org.postgresql.Connection.getObject() to create the
   * object.
   */
  public PGobject() {
  }

  /**
   * This method sets the type of this object.
   *
   * <p>
   * It should not be extended by subclasses, hence its final
   *
   * @param type
   *          a string describing the type of the object
   */
  public final void setType(String type) {
    this.type = type;
  }

  /**
   * This method sets the value of this object. It must be overidden.
   *
   * @param value
   *          a string representation of the value of the object
   * @exception SQLException
   *              thrown if value is invalid for this type
   */
  public void setValue(String value) throws SQLException {
    this.value = value;
  }

  /**
   * As this cannot change during the life of the object, it's final.
   * 
   * @return the type name of this object
   */
  public final String getType() {
    return type;
  }

  /**
   * This must be overidden, to return the value of the object, in the
   * form required by org.postgresql.
   * 
   * @return the value of this object
   */
  public String getValue() {
    return value;
  }

  /**
   * This must be overidden to allow comparisons of objects
   * 
   * @param obj
   *          Object to compare with
   * @return true if the two boxes are identical
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PGobject) {
      final Object otherValue = ((PGobject) obj).getValue();

      if (otherValue == null) {
        return getValue() == null;
      }
      return otherValue.equals(getValue());
    }
    return false;
  }

  /**
   * This must be overidden to allow the object to be cloned
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  /**
   * This is defined here, so user code need not overide it.
   * 
   * @return the value of this object, in the syntax expected by org.postgresql
   */
  @Override
  public String toString() {
    return getValue();
  }

  /**
   * Compute hash. As equals() use only value. Return the same hash for the same value.
   * 
   * @return Value hashcode, 0 if value is null {@link java.util.Objects#hashCode(Object)}
   */
  @Override
  public int hashCode() {
    return getValue() != null ? getValue().hashCode() : 0;
  }
}
