package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_SUPPORTED;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerce;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToBigDecimal;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToBlob;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToBoolean;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToByte;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToBytes;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToDate;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToDouble;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToFloat;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToInt;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToLong;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToShort;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToString;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToTime;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToTimestamp;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToURL;
import static java.nio.charset.StandardCharsets.US_ASCII;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.TimeZone;

import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.CompositeType.Attribute;

public class PGSQLInput implements SQLInput {
	
	private PGConnection connection;
	private CompositeType type;
	private Map<String, Class<?>> typeMap;
	private int currentAttrIdx;
	private Object[] attributeValues;
	private Boolean nullFlag;
	
	public PGSQLInput(PGConnection connection, CompositeType type, Map<String, Class<?>> typeMap, Object[] attributeValues) {
		this.connection = connection;
		this.type = type;
		this.typeMap = typeMap;
		this.attributeValues = attributeValues;
	}

	public Object[] getAttributeValues() {
		return attributeValues;
	}

	private Object getNextAttributeValue() {
		
		Object val = attributeValues[currentAttrIdx++];
		nullFlag = val == null;
		return val;
	}

	@Override
	public String readString() throws SQLException {
		return coerceToString(getNextAttributeValue(), connection);
	}

	@Override
	public boolean readBoolean() throws SQLException {
		return coerceToBoolean(getNextAttributeValue());
	}

	@Override
	public byte readByte() throws SQLException {
		return coerceToByte(getNextAttributeValue());
	}

	@Override
	public short readShort() throws SQLException {
		return coerceToShort(getNextAttributeValue());
	}

	@Override
	public int readInt() throws SQLException {
		return coerceToInt(getNextAttributeValue());
	}

	@Override
	public long readLong() throws SQLException {
		return coerceToLong(getNextAttributeValue());
	}

	@Override
	public float readFloat() throws SQLException {
		return coerceToFloat(getNextAttributeValue());
	}

	@Override
	public double readDouble() throws SQLException {
		return coerceToDouble(getNextAttributeValue());
	}

	@Override
	public BigDecimal readBigDecimal() throws SQLException {
		return coerceToBigDecimal(getNextAttributeValue());
	}

	@Override
	public byte[] readBytes() throws SQLException {

		Object val = getNextAttributeValue();
		if(val == null) {
			return null;
		}
		
		Attribute attr = type.getAttribute(currentAttrIdx);
		if(attr == null) {
			throw new SQLException("Invalid input request (type not array)");
		}
		
		return coerceToBytes(getNextAttributeValue(), attr.type, connection);
	}

	@Override
	public Date readDate() throws SQLException {
		return coerceToDate(getNextAttributeValue(), TimeZone.getDefault(), connection);
	}

	@Override
	public Time readTime() throws SQLException {

		return coerceToTime(getNextAttributeValue(), TimeZone.getDefault(), connection);
	}

	@Override
	public Timestamp readTimestamp() throws SQLException {

		return coerceToTimestamp(getNextAttributeValue(), TimeZone.getDefault(), connection);
	}

	@Override
	public Reader readCharacterStream() throws SQLException {
		return new StringReader(coerceToString(getNextAttributeValue(), connection));
	}

	@Override
	public InputStream readAsciiStream() throws SQLException {
		return new ByteArrayInputStream(coerceToString(getNextAttributeValue(), connection).getBytes(US_ASCII));
	}

	@Override
	public InputStream readBinaryStream() throws SQLException {
		return new ByteArrayInputStream(readBytes());
	}

	@Override
	public Object readObject() throws SQLException {

		Object val = getNextAttributeValue();
		if(val == null) {
			return null;
		}
		
		Attribute attr = type.getAttribute(currentAttrIdx);
		if(attr == null) {
			throw new SQLException("Invalid input request (type not array)");
		}
		
		return coerce(val, attr.type, typeMap, connection);
	}

	@Override
	public Ref readRef() throws SQLException {
		throw NOT_IMPLEMENTED;
	}

	@Override
	public Blob readBlob() throws SQLException {
		return coerceToBlob(getNextAttributeValue(), connection);
	}

	@Override
	public Array readArray() throws SQLException {

		Object val = getNextAttributeValue();
		if(val == null) {
			return null;
		}
		
		Attribute attr = type.getAttribute(currentAttrIdx);
		if(attr == null || attr.type instanceof ArrayType == false || val instanceof Object[] == false) {
			throw new SQLException("Invalid input request (type not array)");
		}
		
		return new PGArray(connection, (ArrayType)attr.type, (Object[])val);
	}

	@Override
	public URL readURL() throws SQLException {
		return coerceToURL(getNextAttributeValue());
	}

	@Override
	public SQLXML readSQLXML() throws SQLException {
		throw NOT_IMPLEMENTED;
	}

	@Override
	public RowId readRowId() throws SQLException {
		throw NOT_IMPLEMENTED;
	}

	@Override
	public boolean wasNull() throws SQLException {
		
		if(nullFlag == null)
			throw new SQLException("no value read");
		
		return nullFlag == true;
	}

	@Override
	public Clob readClob() throws SQLException {
		throw NOT_IMPLEMENTED;
	}

	@Override
	public NClob readNClob() throws SQLException {
		throw NOT_SUPPORTED;
	}

	@Override
	public String readNString() throws SQLException {
		throw NOT_SUPPORTED;
	}

}
