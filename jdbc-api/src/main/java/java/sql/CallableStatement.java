/*
 * Copyright (c) 1996, 2013, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package java.sql;

import java.math.BigDecimal;
import java.util.Calendar;
import java.io.Reader;
import java.io.InputStream;

/**
 * The interface used to execute SQL stored procedures.  The JDBC API
 * provides a stored procedure SQL escape syntax that allows stored procedures
 * to be called in a standard way for all RDBMSs. This escape syntax has one
 * form that includes a result parameter and one that does not. If used, the result
 * parameter must be registered as an OUT parameter. The other parameters
 * can be used for input, output or both. Parameters are referred to
 * sequentially, by number, with the first parameter being 1.
 * <PRE>
 *   {?= call &lt;procedure-name&gt;[(&lt;arg1&gt;,&lt;arg2&gt;, ...)]}
 *   {call &lt;procedure-name&gt;[(&lt;arg1&gt;,&lt;arg2&gt;, ...)]}
 * </PRE>
 * <P>
 * IN parameter values are set using the <code>set</code> methods inherited from
 * {@link java.sql.PreparedStatement}.  The type of all OUT parameters must be
 * registered prior to executing the stored procedure; their values
 * are retrieved after execution via the <code>get</code> methods provided here.
 * <P>
 * A <code>CallableStatement</code> can return one {@link java.sql.ResultSet} object or
 * multiple <code>ResultSet</code> objects.  Multiple
 * <code>ResultSet</code> objects are handled using operations
 * inherited from {@link Statement}.
 * <P>
 * For maximum portability, a call's <code>ResultSet</code> objects and
 * update counts should be processed prior to getting the values of output
 * parameters.
 *
 *
 * @see Connection#prepareCall
 * @see ResultSet
 */

public interface CallableStatement extends PreparedStatement {

    /**
     * Registers the OUT parameter in ordinal position
     * <code>parameterIndex</code> to the JDBC type
     * <code>sqlType</code>.  All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by <code>sqlType</code> for an OUT
     * parameter determines the Java type that must be used
     * in the <code>get</code> method to read the value of that parameter.
     * <p>
     * If the JDBC type expected to be returned to this output parameter
     * is specific to this particular database, <code>sqlType</code>
     * should be <code>Types.OTHER</code>.  The method
     * {@link #getObject} retrieves the value.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @param sqlType the JDBC type code defined by <code>Types</code>.
     *        If the parameter is of JDBC type <code>NUMERIC</code>
     *        or <code>DECIMAL</code>, the version of
     *        <code>registerOutParameter</code> that accepts a scale value
     *        should be used.
     *
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if <code>sqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type
     * @see java.sql.Types
     */
    void registerOutParameter(int parameterIndex, int sqlType)
        throws java.sql.SQLException;

    /**
     * Registers the parameter in ordinal position
     * <code>parameterIndex</code> to be of JDBC type
     * <code>sqlType</code>. All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by <code>sqlType</code> for an OUT
     * parameter determines the Java type that must be used
     * in the <code>get</code> method to read the value of that parameter.
     * <p>
     * This version of <code>registerOutParameter</code> should be
     * used when the parameter is of JDBC type <code>NUMERIC</code>
     * or <code>DECIMAL</code>.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @param sqlType the SQL type code defined by <code>Types</code>.
     * @param scale the desired number of digits to the right of the
     * decimal point.  It must be greater than or equal to zero.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if <code>sqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type
     * @see java.sql.Types
     */
    void registerOutParameter(int parameterIndex, int sqlType, int scale)
        throws java.sql.SQLException;

    /**
     * Retrieves whether the last OUT parameter read had the value of
     * SQL <code>NULL</code>.  Note that this method should be called only after
     * calling a getter method; otherwise, there is no value to use in
     * determining whether it is <code>null</code> or not.
     *
     * @return <code>true</code> if the last parameter read was SQL
     * <code>NULL</code>; <code>false</code> otherwise
     * @exception java.sql.SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     */
    boolean wasNull() throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>CHAR</code>,
     * <code>VARCHAR</code>, or <code>LONGVARCHAR</code> parameter as a
     * <code>String</code> in the Java programming language.
     * <p>
     * For the fixed-length type JDBC <code>CHAR</code>,
     * the <code>String</code> object
     * returned has exactly the same value the SQL
     * <code>CHAR</code> value had in the
     * database, including any padding added by the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value. If the value is SQL <code>NULL</code>,
     *         the result
     *         is <code>null</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setString
     */
    String getString(int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>BIT</code>
     * or <code>BOOLEAN</code> parameter as a
     * <code>boolean</code> in the Java programming language.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     *         the result is <code>false</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setBoolean
     */
    boolean getBoolean(int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>TINYINT</code> parameter
     * as a <code>byte</code> in the Java programming language.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>0</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setByte
     */
    byte getByte(int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>SMALLINT</code> parameter
     * as a <code>short</code> in the Java programming language.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>0</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setShort
     */
    short getShort(int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>INTEGER</code> parameter
     * as an <code>int</code> in the Java programming language.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>0</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setInt
     */
    int getInt(int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>BIGINT</code> parameter
     * as a <code>long</code> in the Java programming language.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>0</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setLong
     */
    long getLong(int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>FLOAT</code> parameter
     * as a <code>float</code> in the Java programming language.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>0</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setFloat
     */
    float getFloat(int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>DOUBLE</code> parameter as a <code>double</code>
     * in the Java programming language.
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>0</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setDouble
     */
    double getDouble(int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>NUMERIC</code> parameter as a
     * <code>java.math.BigDecimal</code> object with <i>scale</i> digits to
     * the right of the decimal point.
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @param scale the number of digits to the right of the decimal point
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @deprecated use <code>getBigDecimal(int parameterIndex)</code>
     *             or <code>getBigDecimal(String parameterName)</code>
     * @see #setBigDecimal
     */
    @Deprecated
    BigDecimal getBigDecimal(int parameterIndex, int scale)
        throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>BINARY</code> or
     * <code>VARBINARY</code> parameter as an array of <code>byte</code>
     * values in the Java programming language.
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setBytes
     */
    byte[] getBytes(int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>DATE</code> parameter as a
     * <code>Date</code> object.
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setDate
     */
    Date getDate(int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>TIME</code> parameter as a
     * <code>Time</code> object.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setTime
     */
    Time getTime(int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>TIMESTAMP</code> parameter as a
     * <code>Timestamp</code> object.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setTimestamp
     */
    Timestamp getTimestamp(int parameterIndex)
        throws java.sql.SQLException;

    //----------------------------------------------------------------------
    // Advanced features:


    /**
     * Retrieves the value of the designated parameter as an <code>Object</code>
     * in the Java programming language. If the value is an SQL <code>NULL</code>,
     * the driver returns a Java <code>null</code>.
     * <p>
     * This method returns a Java object whose type corresponds to the JDBC
     * type that was registered for this parameter using the method
     * <code>registerOutParameter</code>.  By registering the target JDBC
     * type as <code>Types.OTHER</code>, this method can be used
     * to read database-specific abstract data types.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return A <code>java.lang.Object</code> holding the OUT parameter value
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see java.sql.Types
     * @see #setObject
     */
    Object getObject(int parameterIndex) throws java.sql.SQLException;


    //--------------------------JDBC 2.0-----------------------------

    /**
     * Retrieves the value of the designated JDBC <code>NUMERIC</code> parameter as a
     * <code>java.math.BigDecimal</code> object with as many digits to the
     * right of the decimal point as the value contains.
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value in full precision.  If the value is
     * SQL <code>NULL</code>, the result is <code>null</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setBigDecimal
     * @since 1.2
     */
    BigDecimal getBigDecimal(int parameterIndex) throws java.sql.SQLException;

    /**
     * Returns an object representing the value of OUT parameter
     * <code>parameterIndex</code> and uses <code>map</code> for the custom
     * mapping of the parameter value.
     * <p>
     * This method returns a Java object whose type corresponds to the
     * JDBC type that was registered for this parameter using the method
     * <code>registerOutParameter</code>.  By registering the target
     * JDBC type as <code>Types.OTHER</code>, this method can
     * be used to read database-specific abstract data types.
     * @param parameterIndex the first parameter is 1, the second is 2, and so on
     * @param map the mapping from SQL type names to Java classes
     * @return a <code>java.lang.Object</code> holding the OUT parameter value
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setObject
     * @since 1.2
     */
    Object getObject(int parameterIndex, java.util.Map<String,Class<?>> map)
        throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>REF(&lt;structured-type&gt;)</code>
     * parameter as a {@link java.sql.Ref} object in the Java programming language.
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value as a <code>Ref</code> object in the
     * Java programming language.  If the value was SQL <code>NULL</code>, the value
     * <code>null</code> is returned.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.2
     */
    java.sql.Ref getRef (int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>BLOB</code> parameter as a
     * {@link java.sql.Blob} object in the Java programming language.
     * @param parameterIndex the first parameter is 1, the second is 2, and so on
     * @return the parameter value as a <code>Blob</code> object in the
     * Java programming language.  If the value was SQL <code>NULL</code>, the value
     * <code>null</code> is returned.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.2
     */
    java.sql.Blob getBlob (int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>CLOB</code> parameter as a
     * <code>Clob</code> object in the Java programming language.
     * @param parameterIndex the first parameter is 1, the second is 2, and
     * so on
     * @return the parameter value as a <code>Clob</code> object in the
     * Java programming language.  If the value was SQL <code>NULL</code>, the
     * value <code>null</code> is returned.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.2
     */
    java.sql.Clob getClob (int parameterIndex) throws java.sql.SQLException;

    /**
     *
     * Retrieves the value of the designated JDBC <code>ARRAY</code> parameter as an
     * {@link java.sql.Array} object in the Java programming language.
     * @param parameterIndex the first parameter is 1, the second is 2, and
     * so on
     * @return the parameter value as an <code>Array</code> object in
     * the Java programming language.  If the value was SQL <code>NULL</code>, the
     * value <code>null</code> is returned.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.2
     */
    java.sql.Array getArray (int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>DATE</code> parameter as a
     * <code>Date</code> object, using
     * the given <code>Calendar</code> object
     * to construct the date.
     * With a <code>Calendar</code> object, the driver
     * can calculate the date taking into account a custom timezone and locale.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the date
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setDate
     * @since 1.2
     */
    Date getDate(int parameterIndex, Calendar cal)
        throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>TIME</code> parameter as a
     * <code>Time</code> object, using
     * the given <code>Calendar</code> object
     * to construct the time.
     * With a <code>Calendar</code> object, the driver
     * can calculate the time taking into account a custom timezone and locale.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the time
     * @return the parameter value; if the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setTime
     * @since 1.2
     */
    Time getTime(int parameterIndex, Calendar cal)
        throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>TIMESTAMP</code> parameter as a
     * <code>Timestamp</code> object, using
     * the given <code>Calendar</code> object to construct
     * the <code>Timestamp</code> object.
     * With a <code>Calendar</code> object, the driver
     * can calculate the timestamp taking into account a custom timezone and locale.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the timestamp
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setTimestamp
     * @since 1.2
     */
    Timestamp getTimestamp(int parameterIndex, Calendar cal)
        throws java.sql.SQLException;


    /**
     * Registers the designated output parameter.
     * This version of
     * the method <code>registerOutParameter</code>
     * should be used for a user-defined or <code>REF</code> output parameter.  Examples
     * of user-defined types include: <code>STRUCT</code>, <code>DISTINCT</code>,
     * <code>JAVA_OBJECT</code>, and named array types.
     *<p>
     * All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>  For a user-defined parameter, the fully-qualified SQL
     * type name of the parameter should also be given, while a <code>REF</code>
     * parameter requires that the fully-qualified type name of the
     * referenced type be given.  A JDBC driver that does not need the
     * type code and type name information may ignore it.   To be portable,
     * however, applications should always provide these values for
     * user-defined and <code>REF</code> parameters.
     *
     * Although it is intended for user-defined and <code>REF</code> parameters,
     * this method may be used to register a parameter of any JDBC type.
     * If the parameter does not have a user-defined or <code>REF</code> type, the
     * <i>typeName</i> parameter is ignored.
     *
     * <P><B>Note:</B> When reading the value of an out parameter, you
     * must use the getter method whose Java type corresponds to the
     * parameter's registered SQL type.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,...
     * @param sqlType a value from {@link java.sql.Types}
     * @param typeName the fully-qualified name of an SQL structured type
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if <code>sqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type
     * @see java.sql.Types
     * @since 1.2
     */
    void registerOutParameter (int parameterIndex, int sqlType, String typeName)
        throws java.sql.SQLException;

  //--------------------------JDBC 3.0-----------------------------

    /**
     * Registers the OUT parameter named
     * <code>parameterName</code> to the JDBC type
     * <code>sqlType</code>.  All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by <code>sqlType</code> for an OUT
     * parameter determines the Java type that must be used
     * in the <code>get</code> method to read the value of that parameter.
     * <p>
     * If the JDBC type expected to be returned to this output parameter
     * is specific to this particular database, <code>sqlType</code>
     * should be <code>Types.OTHER</code>.  The method
     * {@link #getObject} retrieves the value.
     * @param parameterName the name of the parameter
     * @param sqlType the JDBC type code defined by <code>Types</code>.
     * If the parameter is of JDBC type <code>NUMERIC</code>
     * or <code>DECIMAL</code>, the version of
     * <code>registerOutParameter</code> that accepts a scale value
     * should be used.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if <code>sqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type or if the JDBC driver does not support
     * this method
     * @since 1.4
     * @see java.sql.Types
     */
    void registerOutParameter(String parameterName, int sqlType)
        throws java.sql.SQLException;

    /**
     * Registers the parameter named
     * <code>parameterName</code> to be of JDBC type
     * <code>sqlType</code>.  All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by <code>sqlType</code> for an OUT
     * parameter determines the Java type that must be used
     * in the <code>get</code> method to read the value of that parameter.
     * <p>
     * This version of <code>registerOutParameter</code> should be
     * used when the parameter is of JDBC type <code>NUMERIC</code>
     * or <code>DECIMAL</code>.
     *
     * @param parameterName the name of the parameter
     * @param sqlType SQL type code defined by <code>Types</code>.
     * @param scale the desired number of digits to the right of the
     * decimal point.  It must be greater than or equal to zero.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if <code>sqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type or if the JDBC driver does not support
     * this method
     * @since 1.4
     * @see java.sql.Types
     */
    void registerOutParameter(String parameterName, int sqlType, int scale)
        throws java.sql.SQLException;

    /**
     * Registers the designated output parameter.  This version of
     * the method <code>registerOutParameter</code>
     * should be used for a user-named or REF output parameter.  Examples
     * of user-named types include: STRUCT, DISTINCT, JAVA_OBJECT, and
     * named array types.
     *<p>
     * All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * For a user-named parameter the fully-qualified SQL
     * type name of the parameter should also be given, while a REF
     * parameter requires that the fully-qualified type name of the
     * referenced type be given.  A JDBC driver that does not need the
     * type code and type name information may ignore it.   To be portable,
     * however, applications should always provide these values for
     * user-named and REF parameters.
     *
     * Although it is intended for user-named and REF parameters,
     * this method may be used to register a parameter of any JDBC type.
     * If the parameter does not have a user-named or REF type, the
     * typeName parameter is ignored.
     *
     * <P><B>Note:</B> When reading the value of an out parameter, you
     * must use the <code>getXXX</code> method whose Java type XXX corresponds to the
     * parameter's registered SQL type.
     *
     * @param parameterName the name of the parameter
     * @param sqlType a value from {@link java.sql.Types}
     * @param typeName the fully-qualified name of an SQL structured type
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if <code>sqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type or if the JDBC driver does not support
     * this method
     * @see java.sql.Types
     * @since 1.4
     */
    void registerOutParameter (String parameterName, int sqlType, String typeName)
        throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>DATALINK</code> parameter as a
     * <code>java.net.URL</code> object.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,...
     * @return a <code>java.net.URL</code> object that represents the
     *         JDBC <code>DATALINK</code> value used as the designated
     *         parameter
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs,
     * this method is called on a closed <code>CallableStatement</code>,
     *            or if the URL being returned is
     *            not a valid URL on the Java platform
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setURL
     * @since 1.4
     */
    java.net.URL getURL(int parameterIndex) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given <code>java.net.URL</code> object.
     * The driver converts this to an SQL <code>DATALINK</code> value when
     * it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param val the parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs;
     * this method is called on a closed <code>CallableStatement</code>
     *            or if a URL is malformed
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getURL
     * @since 1.4
     */
    void setURL(String parameterName, java.net.URL val) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to SQL <code>NULL</code>.
     *
     * <P><B>Note:</B> You must specify the parameter's SQL type.
     *
     * @param parameterName the name of the parameter
     * @param sqlType the SQL type code defined in <code>Types</code>
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.4
     */
    void setNull(String parameterName, int sqlType) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given Java <code>boolean</code> value.
     * The driver converts this
     * to an SQL <code>BIT</code> or <code>BOOLEAN</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #getBoolean
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.4
     */
    void setBoolean(String parameterName, boolean x) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given Java <code>byte</code> value.
     * The driver converts this
     * to an SQL <code>TINYINT</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getByte
     * @since 1.4
     */
    void setByte(String parameterName, byte x) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given Java <code>short</code> value.
     * The driver converts this
     * to an SQL <code>SMALLINT</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getShort
     * @since 1.4
     */
    void setShort(String parameterName, short x) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given Java <code>int</code> value.
     * The driver converts this
     * to an SQL <code>INTEGER</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getInt
     * @since 1.4
     */
    void setInt(String parameterName, int x) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given Java <code>long</code> value.
     * The driver converts this
     * to an SQL <code>BIGINT</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getLong
     * @since 1.4
     */
    void setLong(String parameterName, long x) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given Java <code>float</code> value.
     * The driver converts this
     * to an SQL <code>FLOAT</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getFloat
     * @since 1.4
     */
    void setFloat(String parameterName, float x) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given Java <code>double</code> value.
     * The driver converts this
     * to an SQL <code>DOUBLE</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getDouble
     * @since 1.4
     */
    void setDouble(String parameterName, double x) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given
     * <code>java.math.BigDecimal</code> value.
     * The driver converts this to an SQL <code>NUMERIC</code> value when
     * it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getBigDecimal
     * @since 1.4
     */
    void setBigDecimal(String parameterName, BigDecimal x) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given Java <code>String</code> value.
     * The driver converts this
     * to an SQL <code>VARCHAR</code> or <code>LONGVARCHAR</code> value
     * (depending on the argument's
     * size relative to the driver's limits on <code>VARCHAR</code> values)
     * when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getString
     * @since 1.4
     */
    void setString(String parameterName, String x) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given Java array of bytes.
     * The driver converts this to an SQL <code>VARBINARY</code> or
     * <code>LONGVARBINARY</code> (depending on the argument's size relative
     * to the driver's limits on <code>VARBINARY</code> values) when it sends
     * it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getBytes
     * @since 1.4
     */
    void setBytes(String parameterName, byte x[]) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given <code>Date</code> value
     * using the default time zone of the virtual machine that is running
     * the application.
     * The driver converts this
     * to an SQL <code>DATE</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getDate
     * @since 1.4
     */
    void setDate(String parameterName, Date x)
        throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given <code>Time</code> value.
     * The driver converts this
     * to an SQL <code>TIME</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getTime
     * @since 1.4
     */
    void setTime(String parameterName, Time x)
        throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given <code>Timestamp</code> value.
     * The driver
     * converts this to an SQL <code>TIMESTAMP</code> value when it sends it to the
     * database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getTimestamp
     * @since 1.4
     */
    void setTimestamp(String parameterName, Timestamp x)
        throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large ASCII value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code>. Data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterName the name of the parameter
     * @param x the Java input stream that contains the ASCII parameter value
     * @param length the number of bytes in the stream
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.4
     */
    void setAsciiStream(String parameterName, java.io.InputStream x, int length)
        throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large binary value is input to a <code>LONGVARBINARY</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterName the name of the parameter
     * @param x the java input stream which contains the binary parameter value
     * @param length the number of bytes in the stream
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.4
     */
    void setBinaryStream(String parameterName, java.io.InputStream x,
                         int length) throws java.sql.SQLException;

    /**
     * Sets the value of the designated parameter with the given object.
     *
     * <p>The given Java object will be converted to the given targetSqlType
     * before being sent to the database.
     *
     * If the object has a custom mapping (is of a class implementing the
     * interface <code>SQLData</code>),
     * the JDBC driver should call the method <code>SQLData.writeSQL</code> to write it
     * to the SQL data stream.
     * If, on the other hand, the object is of a class implementing
     * <code>Ref</code>, <code>Blob</code>, <code>Clob</code>,  <code>NClob</code>,
     *  <code>Struct</code>, <code>java.net.URL</code>,
     * or <code>Array</code>, the driver should pass it to the database as a
     * value of the corresponding SQL type.
     * <P>
     * Note that this method may be used to pass datatabase-
     * specific abstract data types.
     *
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type (as defined in Types) to be
     * sent to the database. The scale argument may further qualify this type.
     * @param scale for Types.DECIMAL or Types.NUMERIC types,
     *          this is the number of digits after the decimal point.  For all other
     *          types, this value will be ignored.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified targetSqlType
     * @see java.sql.Types
     * @see #getObject
     * @since 1.4
     */
    void setObject(String parameterName, Object x, int targetSqlType, int scale)
        throws java.sql.SQLException;

    /**
     * Sets the value of the designated parameter with the given object.
     *
     * This method is similar to {@link #setObject(String parameterName,
     * Object x, int targetSqlType, int scaleOrLength)},
     * except that it assumes a scale of zero.
     *
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type (as defined in Types) to be
     *                      sent to the database
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified targetSqlType
     * @see #getObject
     * @since 1.4
     */
    void setObject(String parameterName, Object x, int targetSqlType)
        throws java.sql.SQLException;

    /**
     * Sets the value of the designated parameter with the given object.
     *
     * <p>The JDBC specification specifies a standard mapping from
     * Java <code>Object</code> types to SQL types.  The given argument
     * will be converted to the corresponding SQL type before being
     * sent to the database.
     * <p>Note that this method may be used to pass datatabase-
     * specific abstract data types, by using a driver-specific Java
     * type.
     *
     * If the object is of a class implementing the interface <code>SQLData</code>,
     * the JDBC driver should call the method <code>SQLData.writeSQL</code>
     * to write it to the SQL data stream.
     * If, on the other hand, the object is of a class implementing
     * <code>Ref</code>, <code>Blob</code>, <code>Clob</code>,  <code>NClob</code>,
     *  <code>Struct</code>, <code>java.net.URL</code>,
     * or <code>Array</code>, the driver should pass it to the database as a
     * value of the corresponding SQL type.
     * <P>
     * This method throws an exception if there is an ambiguity, for example, if the
     * object is of a class implementing more than one of the interfaces named above.
     *<p>
     *<b>Note:</b> Not all databases allow for a non-typed Null to be sent to
     * the backend. For maximum portability, the <code>setNull</code> or the
     * <code>setObject(String parameterName, Object x, int sqlType)</code>
     * method should be used
     * instead of <code>setObject(String parameterName, Object x)</code>.
     *<p>
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs,
     * this method is called on a closed <code>CallableStatement</code> or if the given
     *            <code>Object</code> parameter is ambiguous
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getObject
     * @since 1.4
     */
    void setObject(String parameterName, Object x) throws java.sql.SQLException;


    /**
     * Sets the designated parameter to the given <code>Reader</code>
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterName the name of the parameter
     * @param reader the <code>java.io.Reader</code> object that
     *        contains the UNICODE data used as the designated parameter
     * @param length the number of characters in the stream
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.4
     */
    void setCharacterStream(String parameterName,
                            java.io.Reader reader,
                            int length) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given <code>Date</code> value,
     * using the given <code>Calendar</code> object.  The driver uses
     * the <code>Calendar</code> object to construct an SQL <code>DATE</code> value,
     * which the driver then sends to the database.  With a
     * a <code>Calendar</code> object, the driver can calculate the date
     * taking into account a custom timezone.  If no
     * <code>Calendar</code> object is specified, the driver uses the default
     * timezone, which is that of the virtual machine running the application.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the date
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getDate
     * @since 1.4
     */
    void setDate(String parameterName, Date x, Calendar cal)
        throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given <code>Time</code> value,
     * using the given <code>Calendar</code> object.  The driver uses
     * the <code>Calendar</code> object to construct an SQL <code>TIME</code> value,
     * which the driver then sends to the database.  With a
     * a <code>Calendar</code> object, the driver can calculate the time
     * taking into account a custom timezone.  If no
     * <code>Calendar</code> object is specified, the driver uses the default
     * timezone, which is that of the virtual machine running the application.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the time
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getTime
     * @since 1.4
     */
    void setTime(String parameterName, Time x, Calendar cal)
        throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given <code>Timestamp</code> value,
     * using the given <code>Calendar</code> object.  The driver uses
     * the <code>Calendar</code> object to construct an SQL <code>TIMESTAMP</code> value,
     * which the driver then sends to the database.  With a
     * a <code>Calendar</code> object, the driver can calculate the timestamp
     * taking into account a custom timezone.  If no
     * <code>Calendar</code> object is specified, the driver uses the default
     * timezone, which is that of the virtual machine running the application.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the timestamp
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getTimestamp
     * @since 1.4
     */
    void setTimestamp(String parameterName, Timestamp x, Calendar cal)
        throws java.sql.SQLException;

    /**
     * Sets the designated parameter to SQL <code>NULL</code>.
     * This version of the method <code>setNull</code> should
     * be used for user-defined types and REF type parameters.  Examples
     * of user-defined types include: STRUCT, DISTINCT, JAVA_OBJECT, and
     * named array types.
     *
     * <P><B>Note:</B> To be portable, applications must give the
     * SQL type code and the fully-qualified SQL type name when specifying
     * a NULL user-defined or REF parameter.  In the case of a user-defined type
     * the name is the type name of the parameter itself.  For a REF
     * parameter, the name is the type name of the referenced type.
     * <p>
     * Although it is intended for user-defined and Ref parameters,
     * this method may be used to set a null parameter of any JDBC type.
     * If the parameter does not have a user-defined or REF type, the given
     * typeName is ignored.
     *
     *
     * @param parameterName the name of the parameter
     * @param sqlType a value from <code>Types</code>
     * @param typeName the fully-qualified name of an SQL user-defined type;
     *        ignored if the parameter is not a user-defined type or
     *        SQL <code>REF</code> value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.4
     */
    void setNull (String parameterName, int sqlType, String typeName)
        throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>CHAR</code>, <code>VARCHAR</code>,
     * or <code>LONGVARCHAR</code> parameter as a <code>String</code> in
     * the Java programming language.
     * <p>
     * For the fixed-length type JDBC <code>CHAR</code>,
     * the <code>String</code> object
     * returned has exactly the same value the SQL
     * <code>CHAR</code> value had in the
     * database, including any padding added by the database.
     * @param parameterName the name of the parameter
     * @return the parameter value. If the value is SQL <code>NULL</code>, the result
     * is <code>null</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setString
     * @since 1.4
     */
    String getString(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>BIT</code> or <code>BOOLEAN</code>
     * parameter as a
     * <code>boolean</code> in the Java programming language.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>false</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setBoolean
     * @since 1.4
     */
    boolean getBoolean(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>TINYINT</code> parameter as a <code>byte</code>
     * in the Java programming language.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>0</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setByte
     * @since 1.4
     */
    byte getByte(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>SMALLINT</code> parameter as a <code>short</code>
     * in the Java programming language.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>0</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setShort
     * @since 1.4
     */
    short getShort(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>INTEGER</code> parameter as an <code>int</code>
     * in the Java programming language.
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     *         the result is <code>0</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setInt
     * @since 1.4
     */
    int getInt(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>BIGINT</code> parameter as a <code>long</code>
     * in the Java programming language.
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     *         the result is <code>0</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setLong
     * @since 1.4
     */
    long getLong(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>FLOAT</code> parameter as a <code>float</code>
     * in the Java programming language.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     *         the result is <code>0</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setFloat
     * @since 1.4
     */
    float getFloat(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>DOUBLE</code> parameter as a <code>double</code>
     * in the Java programming language.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     *         the result is <code>0</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setDouble
     * @since 1.4
     */
    double getDouble(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>BINARY</code> or <code>VARBINARY</code>
     * parameter as an array of <code>byte</code> values in the Java
     * programming language.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result is
     *  <code>null</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setBytes
     * @since 1.4
     */
    byte[] getBytes(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>DATE</code> parameter as a
     * <code>Date</code> object.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>null</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setDate
     * @since 1.4
     */
    Date getDate(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>TIME</code> parameter as a
     * <code>Time</code> object.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>null</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setTime
     * @since 1.4
     */
    Time getTime(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>TIMESTAMP</code> parameter as a
     * <code>Timestamp</code> object.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>null</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setTimestamp
     * @since 1.4
     */
    Timestamp getTimestamp(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a parameter as an <code>Object</code> in the Java
     * programming language. If the value is an SQL <code>NULL</code>, the
     * driver returns a Java <code>null</code>.
     * <p>
     * This method returns a Java object whose type corresponds to the JDBC
     * type that was registered for this parameter using the method
     * <code>registerOutParameter</code>.  By registering the target JDBC
     * type as <code>Types.OTHER</code>, this method can be used
     * to read database-specific abstract data types.
     * @param parameterName the name of the parameter
     * @return A <code>java.lang.Object</code> holding the OUT parameter value.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see Types
     * @see #setObject
     * @since 1.4
     */
    Object getObject(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>NUMERIC</code> parameter as a
     * <code>java.math.BigDecimal</code> object with as many digits to the
     * right of the decimal point as the value contains.
     * @param parameterName the name of the parameter
     * @return the parameter value in full precision.  If the value is
     * SQL <code>NULL</code>, the result is <code>null</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter;  if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setBigDecimal
     * @since 1.4
     */
    BigDecimal getBigDecimal(String parameterName) throws java.sql.SQLException;

    /**
     * Returns an object representing the value of OUT parameter
     * <code>parameterName</code> and uses <code>map</code> for the custom
     * mapping of the parameter value.
     * <p>
     * This method returns a Java object whose type corresponds to the
     * JDBC type that was registered for this parameter using the method
     * <code>registerOutParameter</code>.  By registering the target
     * JDBC type as <code>Types.OTHER</code>, this method can
     * be used to read database-specific abstract data types.
     * @param parameterName the name of the parameter
     * @param map the mapping from SQL type names to Java classes
     * @return a <code>java.lang.Object</code> holding the OUT parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setObject
     * @since 1.4
     */
    Object getObject(String parameterName, java.util.Map<String,Class<?>> map)
      throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>REF(&lt;structured-type&gt;)</code>
     * parameter as a {@link java.sql.Ref} object in the Java programming language.
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a <code>Ref</code> object in the
     *         Java programming language.  If the value was SQL <code>NULL</code>,
     *         the value <code>null</code> is returned.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.4
     */
    Ref getRef (String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>BLOB</code> parameter as a
     * {@link java.sql.Blob} object in the Java programming language.
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a <code>Blob</code> object in the
     *         Java programming language.  If the value was SQL <code>NULL</code>,
     *         the value <code>null</code> is returned.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.4
     */
    java.sql.Blob getBlob (String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>CLOB</code> parameter as a
     * <code>Clob</code> object in the Java programming language.
     * @param parameterName the name of the parameter
     * @return the parameter value as a <code>Clob</code> object in the
     *         Java programming language.  If the value was SQL <code>NULL</code>,
     *         the value <code>null</code> is returned.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.4
     */
    java.sql.Clob getClob (String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>ARRAY</code> parameter as an
     * {@link java.sql.Array} object in the Java programming language.
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as an <code>Array</code> object in
     *         Java programming language.  If the value was SQL <code>NULL</code>,
     *         the value <code>null</code> is returned.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.4
     */
    Array getArray (String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>DATE</code> parameter as a
     * <code>Date</code> object, using
     * the given <code>Calendar</code> object
     * to construct the date.
     * With a <code>Calendar</code> object, the driver
     * can calculate the date taking into account a custom timezone and locale.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     * @param parameterName the name of the parameter
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the date
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     * the result is <code>null</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setDate
     * @since 1.4
     */
    Date getDate(String parameterName, Calendar cal)
        throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>TIME</code> parameter as a
     * <code>Time</code> object, using
     * the given <code>Calendar</code> object
     * to construct the time.
     * With a <code>Calendar</code> object, the driver
     * can calculate the time taking into account a custom timezone and locale.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     * @param parameterName the name of the parameter
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the time
     * @return the parameter value; if the value is SQL <code>NULL</code>, the result is
     * <code>null</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setTime
     * @since 1.4
     */
    Time getTime(String parameterName, Calendar cal)
        throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>TIMESTAMP</code> parameter as a
     * <code>Timestamp</code> object, using
     * the given <code>Calendar</code> object to construct
     * the <code>Timestamp</code> object.
     * With a <code>Calendar</code> object, the driver
     * can calculate the timestamp taking into account a custom timezone and locale.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     *
     * @param parameterName the name of the parameter
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the timestamp
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result is
     * <code>null</code>.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setTimestamp
     * @since 1.4
     */
    Timestamp getTimestamp(String parameterName, Calendar cal)
        throws java.sql.SQLException;

    /**
     * Retrieves the value of a JDBC <code>DATALINK</code> parameter as a
     * <code>java.net.URL</code> object.
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a <code>java.net.URL</code> object in the
     * Java programming language.  If the value was SQL <code>NULL</code>, the
     * value <code>null</code> is returned.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs,
     * this method is called on a closed <code>CallableStatement</code>,
     *            or if there is a problem with the URL
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setURL
     * @since 1.4
     */
    java.net.URL getURL(String parameterName) throws java.sql.SQLException;

    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * Retrieves the value of the designated JDBC <code>ROWID</code> parameter as a
     * <code>RowId</code> object.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,...
     * @return a <code>RowId</code> object that represents the JDBC <code>ROWID</code>
     *     value is used as the designated parameter. If the parameter contains
     * a SQL <code>NULL</code>, then a <code>null</code> value is returned.
     * @throws java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    java.sql.RowId getRowId(int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>ROWID</code> parameter as a
     * <code>RowId</code> object.
     *
     * @param parameterName the name of the parameter
     * @return a <code>RowId</code> object that represents the JDBC <code>ROWID</code>
     *     value is used as the designated parameter. If the parameter contains
     * a SQL <code>NULL</code>, then a <code>null</code> value is returned.
     * @throws java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    java.sql.RowId getRowId(String parameterName) throws java.sql.SQLException;

     /**
     * Sets the designated parameter to the given <code>RowId</code> object. The
     * driver converts this to a SQL <code>ROWID</code> when it sends it to the
     * database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    void setRowId(String parameterName, RowId x) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given <code>String</code> object.
     * The driver converts this to a SQL <code>NCHAR</code> or
     * <code>NVARCHAR</code> or <code>LONGNVARCHAR</code>
     * @param parameterName the name of the parameter to be set
     * @param value the parameter value
     * @throws java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    void setNString(String parameterName, String value)
            throws java.sql.SQLException;

    /**
     * Sets the designated parameter to a <code>Reader</code> object. The
     * <code>Reader</code> reads the data till end-of-file is reached. The
     * driver does the necessary conversion from Java character format to
     * the national character set in the database.
     * @param parameterName the name of the parameter to be set
     * @param value the parameter value
     * @param length the number of characters in the parameter data.
     * @throws java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    void setNCharacterStream(String parameterName, Reader value, long length)
            throws java.sql.SQLException;

     /**
     * Sets the designated parameter to a <code>NClob</code> object. The object
     * implements the <code>NClob</code> interface. This <code>NClob</code>
     * object maps to a SQL <code>NCLOB</code>.
     * @param parameterName the name of the parameter to be set
     * @param value the parameter value
     * @throws java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
     void setNClob(String parameterName, java.sql.NClob value) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to a <code>Reader</code> object.  The <code>reader</code> must contain  the number
     * of characters specified by length otherwise a <code>SQLException</code> will be
     * generated when the <code>CallableStatement</code> is executed.
     * This method differs from the <code>setCharacterStream (int, Reader, int)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>CLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be send to the server as a <code>LONGVARCHAR</code> or a <code>CLOB</code>
     * @param parameterName the name of the parameter to be set
     * @param reader An object that contains the data to set the parameter value to.
     * @param length the number of characters in the parameter data.
     * @throws java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if the length specified is less than zero;
     * a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     *
     * @since 1.6
     */
     void setClob(String parameterName, Reader reader, long length)
       throws java.sql.SQLException;

    /**
     * Sets the designated parameter to a <code>InputStream</code> object.  The <code>inputstream</code> must contain  the number
     * of characters specified by length, otherwise a <code>SQLException</code> will be
     * generated when the <code>CallableStatement</code> is executed.
     * This method differs from the <code>setBinaryStream (int, InputStream, int)</code>
     * method because it informs the driver that the parameter value should be
     * sent to the server as a <code>BLOB</code>.  When the <code>setBinaryStream</code> method is used,
     * the driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a <code>LONGVARBINARY</code> or a <code>BLOB</code>
     *
     * @param parameterName the name of the parameter to be set
     * the second is 2, ...
     *
     * @param inputStream An object that contains the data to set the parameter
     * value to.
     * @param length the number of bytes in the parameter data.
     * @throws java.sql.SQLException  if parameterName does not correspond to a named
     * parameter; if the length specified
     * is less than zero; if the number of bytes in the inputstream does not match
     * the specified length; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     *
     * @since 1.6
     */
     void setBlob(String parameterName, InputStream inputStream, long length)
        throws java.sql.SQLException;
    /**
     * Sets the designated parameter to a <code>Reader</code> object.  The <code>reader</code> must contain  the number
     * of characters specified by length otherwise a <code>SQLException</code> will be
     * generated when the <code>CallableStatement</code> is executed.
     * This method differs from the <code>setCharacterStream (int, Reader, int)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>NCLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be send to the server as a <code>LONGNVARCHAR</code> or a <code>NCLOB</code>
     *
     * @param parameterName the name of the parameter to be set
     * @param reader An object that contains the data to set the parameter value to.
     * @param length the number of characters in the parameter data.
     * @throws java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if the length specified is less than zero;
     * if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
     void setNClob(String parameterName, Reader reader, long length)
       throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated JDBC <code>NCLOB</code> parameter as a
     * <code>NClob</code> object in the Java programming language.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, and
     * so on
     * @return the parameter value as a <code>NClob</code> object in the
     * Java programming language.  If the value was SQL <code>NULL</code>, the
     * value <code>null</code> is returned.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    java.sql.NClob getNClob (int parameterIndex) throws java.sql.SQLException;


    /**
     * Retrieves the value of a JDBC <code>NCLOB</code> parameter as a
     * <code>NClob</code> object in the Java programming language.
     * @param parameterName the name of the parameter
     * @return the parameter value as a <code>NClob</code> object in the
     *         Java programming language.  If the value was SQL <code>NULL</code>,
     *         the value <code>null</code> is returned.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    NClob getNClob (String parameterName) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given <code>SQLXML</code> object. The driver converts this to an
     * <code>SQL XML</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param xmlObject a <code>SQLXML</code> object that maps an <code>SQL XML</code> value
     * @throws java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs;
     * this method is called on a closed <code>CallableStatement</code> or
     * the <code>java.xml.transform.Result</code>,
   *  <code>Writer</code> or <code>OutputStream</code> has not been closed for the <code>SQLXML</code> object
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     *
     * @since 1.6
     */
    void setSQLXML(String parameterName, java.sql.SQLXML xmlObject) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated <code>SQL XML</code> parameter as a
     * <code>SQLXML</code> object in the Java programming language.
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @return a <code>SQLXML</code> object that maps an <code>SQL XML</code> value
     * @throws java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    java.sql.SQLXML getSQLXML(int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated <code>SQL XML</code> parameter as a
     * <code>SQLXML</code> object in the Java programming language.
     * @param parameterName the name of the parameter
     * @return a <code>SQLXML</code> object that maps an <code>SQL XML</code> value
     * @throws java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    SQLXML getSQLXML(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated <code>NCHAR</code>,
     * <code>NVARCHAR</code>
     * or <code>LONGNVARCHAR</code> parameter as
     * a <code>String</code> in the Java programming language.
     *  <p>
     * For the fixed-length type JDBC <code>NCHAR</code>,
     * the <code>String</code> object
     * returned has exactly the same value the SQL
     * <code>NCHAR</code> value had in the
     * database, including any padding added by the database.
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @return a <code>String</code> object that maps an
     * <code>NCHAR</code>, <code>NVARCHAR</code> or <code>LONGNVARCHAR</code> value
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     * @see #setNString
     */
    String getNString(int parameterIndex) throws java.sql.SQLException;


    /**
     *  Retrieves the value of the designated <code>NCHAR</code>,
     * <code>NVARCHAR</code>
     * or <code>LONGNVARCHAR</code> parameter as
     * a <code>String</code> in the Java programming language.
     * <p>
     * For the fixed-length type JDBC <code>NCHAR</code>,
     * the <code>String</code> object
     * returned has exactly the same value the SQL
     * <code>NCHAR</code> value had in the
     * database, including any padding added by the database.
     *
     * @param parameterName the name of the parameter
     * @return a <code>String</code> object that maps an
     * <code>NCHAR</code>, <code>NVARCHAR</code> or <code>LONGNVARCHAR</code> value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     * @see #setNString
     */
    String getNString(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated parameter as a
     * <code>java.io.Reader</code> object in the Java programming language.
     * It is intended for use when
     * accessing  <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> parameters.
     *
     * @return a <code>java.io.Reader</code> object that contains the parameter
     * value; if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code> in the Java programming language.
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    java.io.Reader getNCharacterStream(int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated parameter as a
     * <code>java.io.Reader</code> object in the Java programming language.
     * It is intended for use when
     * accessing  <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> parameters.
     *
     * @param parameterName the name of the parameter
     * @return a <code>java.io.Reader</code> object that contains the parameter
     * value; if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code> in the Java programming language
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    java.io.Reader getNCharacterStream(String parameterName) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated parameter as a
     * <code>java.io.Reader</code> object in the Java programming language.
     *
     * @return a <code>java.io.Reader</code> object that contains the parameter
     * value; if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code> in the Java programming language.
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @exception java.sql.SQLException if the parameterIndex is not valid; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @since 1.6
     */
    java.io.Reader getCharacterStream(int parameterIndex) throws java.sql.SQLException;

    /**
     * Retrieves the value of the designated parameter as a
     * <code>java.io.Reader</code> object in the Java programming language.
     *
     * @param parameterName the name of the parameter
     * @return a <code>java.io.Reader</code> object that contains the parameter
     * value; if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code> in the Java programming language
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    java.io.Reader getCharacterStream(String parameterName) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given <code>Blob</code> object.
     * The driver converts this to an SQL <code>BLOB</code> value when it
     * sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x a <code>Blob</code> object that maps an SQL <code>BLOB</code> value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    void setBlob (String parameterName, Blob x) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given <code>Clob</code> object.
     * The driver converts this to an SQL <code>CLOB</code> value when it
     * sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x a <code>Clob</code> object that maps an SQL <code>CLOB</code> value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    void setClob (String parameterName, Clob x) throws java.sql.SQLException;
    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large ASCII value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code>. Data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterName the name of the parameter
     * @param x the Java input stream that contains the ASCII parameter value
     * @param length the number of bytes in the stream
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    void setAsciiStream(String parameterName, java.io.InputStream x, long length)
        throws java.sql.SQLException;

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large binary value is input to a <code>LONGVARBINARY</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterName the name of the parameter
     * @param x the java input stream which contains the binary parameter value
     * @param length the number of bytes in the stream
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    void setBinaryStream(String parameterName, java.io.InputStream x,
                         long length) throws java.sql.SQLException;
        /**
     * Sets the designated parameter to the given <code>Reader</code>
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterName the name of the parameter
     * @param reader the <code>java.io.Reader</code> object that
     *        contains the UNICODE data used as the designated parameter
     * @param length the number of characters in the stream
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    void setCharacterStream(String parameterName,
                            java.io.Reader reader,
                            long length) throws java.sql.SQLException;
     //--
    /**
     * Sets the designated parameter to the given input stream.
     * When a very large ASCII value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code>. Data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setAsciiStream</code> which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param x the Java input stream that contains the ASCII parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException  if the JDBC driver does not support this method
       * @since 1.6
    */
    void setAsciiStream(String parameterName, java.io.InputStream x)
            throws java.sql.SQLException;
    /**
     * Sets the designated parameter to the given input stream.
     * When a very large binary value is input to a <code>LONGVARBINARY</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code> object. The data will be read from the
     * stream as needed until end-of-file is reached.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setBinaryStream</code> which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param x the java input stream which contains the binary parameter value
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * @since 1.6
     */
    void setBinaryStream(String parameterName, java.io.InputStream x)
    throws java.sql.SQLException;
    /**
     * Sets the designated parameter to the given <code>Reader</code>
     * object.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setCharacterStream</code> which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param reader the <code>java.io.Reader</code> object that contains the
     *        Unicode data
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * @since 1.6
     */
    void setCharacterStream(String parameterName,
                          java.io.Reader reader) throws java.sql.SQLException;
  /**
     * Sets the designated parameter to a <code>Reader</code> object. The
     * <code>Reader</code> reads the data till end-of-file is reached. The
     * driver does the necessary conversion from Java character format to
     * the national character set in the database.

     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setNCharacterStream</code> which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param value the parameter value
     * @throws java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs; or
     * this method is called on a closed <code>CallableStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * @since 1.6
     */
     void setNCharacterStream(String parameterName, Reader value) throws java.sql.SQLException;

    /**
     * Sets the designated parameter to a <code>Reader</code> object.
     * This method differs from the <code>setCharacterStream (int, Reader)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>CLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be send to the server as a <code>LONGVARCHAR</code> or a <code>CLOB</code>
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setClob</code> which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param reader An object that contains the data to set the parameter value to.
     * @throws java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or this method is called on
     * a closed <code>CallableStatement</code>
     *
     * @throws java.sql.SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * @since 1.6
     */
     void setClob(String parameterName, Reader reader)
       throws java.sql.SQLException;

    /**
     * Sets the designated parameter to a <code>InputStream</code> object.
     * This method differs from the <code>setBinaryStream (int, InputStream)</code>
     * method because it informs the driver that the parameter value should be
     * sent to the server as a <code>BLOB</code>.  When the <code>setBinaryStream</code> method is used,
     * the driver may have to do extra work to determine whether the parameter
     * data should be send to the server as a <code>LONGVARBINARY</code> or a <code>BLOB</code>
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setBlob</code> which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param inputStream An object that contains the data to set the parameter
     * value to.
     * @throws java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     *
     * @since 1.6
     */
     void setBlob(String parameterName, InputStream inputStream)
        throws java.sql.SQLException;
    /**
     * Sets the designated parameter to a <code>Reader</code> object.
     * This method differs from the <code>setCharacterStream (int, Reader)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>NCLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be send to the server as a <code>LONGNVARCHAR</code> or a <code>NCLOB</code>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setNClob</code> which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param reader An object that contains the data to set the parameter value to.
     * @throws java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if the driver does not support national character sets;
     * if the driver can detect that a data conversion
     *  error could occur;  if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     *
     * @since 1.6
     */
     void setNClob(String parameterName, Reader reader)
       throws java.sql.SQLException;

    //------------------------- JDBC 4.1 -----------------------------------


    /**
     *<p>Returns an object representing the value of OUT parameter
     * {@code parameterIndex} and will convert from the
     * SQL type of the parameter to the requested Java data type, if the
     * conversion is supported. If the conversion is not
     * supported or null is specified for the type, a
     * <code>SQLException</code> is thrown.
     *<p>
     * At a minimum, an implementation must support the conversions defined in
     * Appendix B, Table B-3 and conversion of appropriate user defined SQL
     * types to a Java type which implements {@code SQLData}, or {@code Struct}.
     * Additional conversions may be supported and are vendor defined.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, and so on
     * @param type Class representing the Java data type to convert the
     * designated parameter to.
     * @param <T> the type of the class modeled by this Class object
     * @return an instance of {@code type} holding the OUT parameter value
     * @throws java.sql.SQLException if conversion is not supported, type is null or
     *         another error occurs. The getCause() method of the
     * exception may provide a more detailed exception, for example, if
     * a conversion error occurs
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.7
     */
     public <T> T getObject(int parameterIndex, Class<T> type) throws java.sql.SQLException;


    /**
     *<p>Returns an object representing the value of OUT parameter
     * {@code parameterName} and will convert from the
     * SQL type of the parameter to the requested Java data type, if the
     * conversion is supported. If the conversion is not
     * supported  or null is specified for the type, a
     * <code>SQLException</code> is thrown.
     *<p>
     * At a minimum, an implementation must support the conversions defined in
     * Appendix B, Table B-3 and conversion of appropriate user defined SQL
     * types to a Java type which implements {@code SQLData}, or {@code Struct}.
     * Additional conversions may be supported and are vendor defined.
     *
     * @param parameterName the name of the parameter
     * @param type Class representing the Java data type to convert
     * the designated parameter to.
     * @param <T> the type of the class modeled by this Class object
     * @return an instance of {@code type} holding the OUT parameter
     * value
     * @throws java.sql.SQLException if conversion is not supported, type is null or
     *         another error occurs. The getCause() method of the
     * exception may provide a more detailed exception, for example, if
     * a conversion error occurs
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.7
     */
     public <T> T getObject(String parameterName, Class<T> type) throws java.sql.SQLException;

     //------------------------- JDBC 4.2 -----------------------------------

     /**
     * <p>Sets the value of the designated parameter with the given object.
     *
     * If the second argument is an {@code InputStream} then the stream
     * must contain the number of bytes specified by scaleOrLength.
     * If the second argument is a {@code Reader} then the reader must
     * contain the number of characters specified
     * by scaleOrLength. If these conditions are not true the driver
     * will generate a
     * {@code SQLException} when the prepared statement is executed.
     *
     * <p>The given Java object will be converted to the given targetSqlType
     * before being sent to the database.
     *
     * If the object has a custom mapping (is of a class implementing the
     * interface {@code SQLData}),
     * the JDBC driver should call the method {@code SQLData.writeSQL} to
     * write it to the SQL data stream.
     * If, on the other hand, the object is of a class implementing
     * {@code Ref}, {@code Blob}, {@code Clob},  {@code NClob},
     *  {@code Struct}, {@code java.net.URL},
     * or {@code Array}, the driver should pass it to the database as a
     * value of the corresponding SQL type.
     *
     * <p>Note that this method may be used to pass database-specific
     * abstract data types.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type to be
     * sent to the database. The scale argument may further qualify this type.
     * @param scaleOrLength for {@code JDBCType.DECIMAL}
     *          or {@code JDBCType.NUMERIC types},
     *          this is the number of digits after the decimal point. For
     *          Java Object types {@code InputStream} and {@code Reader},
     *          this is the length
     *          of the data in the stream or reader.  For all other types,
     *          this value will be ignored.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs
     * or this method is called on a closed {@code CallableStatement}  or
     *            if the Java Object specified by x is an InputStream
     *            or Reader object and the value of the scale parameter is less
     *            than zero
     * @exception java.sql.SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified targetSqlType
     * @see java.sql.JDBCType
     * @see java.sql.SQLType
     *
     * @since 1.8
     */
     default void setObject(String parameterName, Object x, java.sql.SQLType targetSqlType,
                            int scaleOrLength) throws java.sql.SQLException {
        throw new java.sql.SQLFeatureNotSupportedException("setObject not implemented");
    }
    /**
     * Sets the value of the designated parameter with the given object.
     *
     * This method is similar to {@link #setObject(String parameterName,
     * Object x, java.sql.SQLType targetSqlType, int scaleOrLength)},
     * except that it assumes a scale of zero.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type to be sent to the database
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs
     * or this method is called on a closed {@code CallableStatement}
     * @exception java.sql.SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified targetSqlType
     * @see java.sql.JDBCType
     * @see java.sql.SQLType
     * @since 1.8
     */
     default void setObject(String parameterName, Object x, java.sql.SQLType targetSqlType)
        throws java.sql.SQLException {
        throw new java.sql.SQLFeatureNotSupportedException("setObject not implemented");
    }

    /**
     * Registers the OUT parameter in ordinal position
     * {@code parameterIndex} to the JDBC type
     * {@code sqlType}.  All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by {@code sqlType} for an OUT
     * parameter determines the Java type that must be used
     * in the {@code get} method to read the value of that parameter.
     * <p>
     * If the JDBC type expected to be returned to this output parameter
     * is specific to this particular database, {@code sqlType}
     * may be {@code JDBCType.OTHER} or a {@code SQLType} that is supported by
     * the JDBC driver.  The method
     * {@link #getObject} retrieves the value.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @param sqlType the JDBC type code defined by {@code SQLType} to use to
     * register the OUT Parameter.
     *        If the parameter is of JDBC type {@code JDBCType.NUMERIC}
     *        or {@code JDBCType.DECIMAL}, the version of
     *        {@code registerOutParameter} that accepts a scale value
     *        should be used.
     *
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @exception java.sql.SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified sqlType
     * @see java.sql.JDBCType
     * @see java.sql.SQLType
     * @since 1.8
     */
    default void registerOutParameter(int parameterIndex, java.sql.SQLType sqlType)
        throws java.sql.SQLException {
        throw new java.sql.SQLFeatureNotSupportedException("registerOutParameter not implemented");
    }

    /**
     * Registers the parameter in ordinal position
     * {@code parameterIndex} to be of JDBC type
     * {@code sqlType}. All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by {@code sqlType} for an OUT
     * parameter determines the Java type that must be used
     * in the {@code get} method to read the value of that parameter.
     * <p>
     * This version of {@code  registerOutParameter} should be
     * used when the parameter is of JDBC type {@code JDBCType.NUMERIC}
     * or {@code JDBCType.DECIMAL}.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @param sqlType the JDBC type code defined by {@code SQLType} to use to
     * register the OUT Parameter.
     * @param scale the desired number of digits to the right of the
     * decimal point.  It must be greater than or equal to zero.
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @exception java.sql.SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified sqlType
     * @see java.sql.JDBCType
     * @see java.sql.SQLType
     * @since 1.8
     */
    default void registerOutParameter(int parameterIndex, java.sql.SQLType sqlType,
                                      int scale) throws java.sql.SQLException {
        throw new java.sql.SQLFeatureNotSupportedException("registerOutParameter not implemented");
    }
    /**
     * Registers the designated output parameter.
     * This version of
     * the method {@code  registerOutParameter}
     * should be used for a user-defined or {@code REF} output parameter.
     * Examples
     * of user-defined types include: {@code STRUCT}, {@code DISTINCT},
     * {@code JAVA_OBJECT}, and named array types.
     *<p>
     * All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>  For a user-defined parameter, the fully-qualified SQL
     * type name of the parameter should also be given, while a {@code REF}
     * parameter requires that the fully-qualified type name of the
     * referenced type be given.  A JDBC driver that does not need the
     * type code and type name information may ignore it.   To be portable,
     * however, applications should always provide these values for
     * user-defined and {@code REF} parameters.
     *
     * Although it is intended for user-defined and {@code REF} parameters,
     * this method may be used to register a parameter of any JDBC type.
     * If the parameter does not have a user-defined or {@code REF} type, the
     * <i>typeName</i> parameter is ignored.
     *
     * <P><B>Note:</B> When reading the value of an out parameter, you
     * must use the getter method whose Java type corresponds to the
     * parameter's registered SQL type.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterIndex the first parameter is 1, the second is 2,...
     * @param sqlType the JDBC type code defined by {@code SQLType} to use to
     * register the OUT Parameter.
     * @param typeName the fully-qualified name of an SQL structured type
     * @exception java.sql.SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @exception java.sql.SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified sqlType
     * @see java.sql.JDBCType
     * @see java.sql.SQLType
     * @since 1.8
     */
    default void registerOutParameter (int parameterIndex, java.sql.SQLType sqlType,
                                       String typeName) throws java.sql.SQLException {
        throw new java.sql.SQLFeatureNotSupportedException("registerOutParameter not implemented");
    }

    /**
     * Registers the OUT parameter named
     * <code>parameterName</code> to the JDBC type
     * {@code sqlType}.  All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by {@code sqlType} for an OUT
     * parameter determines the Java type that must be used
     * in the {@code get} method to read the value of that parameter.
     * <p>
     * If the JDBC type expected to be returned to this output parameter
     * is specific to this particular database, {@code sqlType}
     * should be {@code JDBCType.OTHER} or a {@code SQLType} that is supported
     * by the JDBC driver..  The method
     * {@link #getObject} retrieves the value.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterName the name of the parameter
     * @param sqlType the JDBC type code defined by {@code SQLType} to use to
     * register the OUT Parameter.
     * If the parameter is of JDBC type {@code JDBCType.NUMERIC}
     * or {@code JDBCType.DECIMAL}, the version of
     * {@code  registerOutParameter} that accepts a scale value
     * should be used.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @exception java.sql.SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified sqlType
     * or if the JDBC driver does not support
     * this method
     * @since 1.8
     * @see java.sql.JDBCType
     * @see java.sql.SQLType
     */
    default void registerOutParameter(String parameterName, java.sql.SQLType sqlType)
        throws java.sql.SQLException {
        throw new java.sql.SQLFeatureNotSupportedException("registerOutParameter not implemented");
    }

    /**
     * Registers the parameter named
     * <code>parameterName</code> to be of JDBC type
     * {@code sqlType}.  All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by {@code sqlType} for an OUT
     * parameter determines the Java type that must be used
     * in the {@code get} method to read the value of that parameter.
     * <p>
     * This version of {@code  registerOutParameter} should be
     * used when the parameter is of JDBC type {@code JDBCType.NUMERIC}
     * or {@code JDBCType.DECIMAL}.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterName the name of the parameter
     * @param sqlType the JDBC type code defined by {@code SQLType} to use to
     * register the OUT Parameter.
     * @param scale the desired number of digits to the right of the
     * decimal point.  It must be greater than or equal to zero.
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @exception java.sql.SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified sqlType
     * or if the JDBC driver does not support
     * this method
     * @since 1.8
     * @see java.sql.JDBCType
     * @see java.sql.SQLType
     */
    default void registerOutParameter(String parameterName, java.sql.SQLType sqlType,
                                      int scale) throws java.sql.SQLException {
        throw new java.sql.SQLFeatureNotSupportedException("registerOutParameter not implemented");
    }

    /**
     * Registers the designated output parameter.  This version of
     * the method {@code  registerOutParameter}
     * should be used for a user-named or REF output parameter.  Examples
     * of user-named types include: STRUCT, DISTINCT, JAVA_OBJECT, and
     * named array types.
     *<p>
     * All OUT parameters must be registered
     * before a stored procedure is executed.
     * </p>
     * For a user-named parameter the fully-qualified SQL
     * type name of the parameter should also be given, while a REF
     * parameter requires that the fully-qualified type name of the
     * referenced type be given.  A JDBC driver that does not need the
     * type code and type name information may ignore it.   To be portable,
     * however, applications should always provide these values for
     * user-named and REF parameters.
     *
     * Although it is intended for user-named and REF parameters,
     * this method may be used to register a parameter of any JDBC type.
     * If the parameter does not have a user-named or REF type, the
     * typeName parameter is ignored.
     *
     * <P><B>Note:</B> When reading the value of an out parameter, you
     * must use the {@code getXXX} method whose Java type XXX corresponds to the
     * parameter's registered SQL type.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterName the name of the parameter
     * @param sqlType the JDBC type code defined by {@code SQLType} to use to
     * register the OUT Parameter.
     * @param typeName the fully-qualified name of an SQL structured type
     * @exception java.sql.SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @exception java.sql.SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified sqlType
     * or if the JDBC driver does not support this method
     * @see JDBCType
     * @see java.sql.SQLType
     * @since 1.8
     */
    default void registerOutParameter (String parameterName, SQLType sqlType,
            String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter not implemented");
    }
}
