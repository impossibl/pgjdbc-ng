package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_SUPPORTED;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerce;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.mapSetType;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLOutput;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.CompositeType.Attribute;

public class PGSQLOutput implements SQLOutput {
	
	private PGConnection connection;
	private CompositeType type;
	private int currentAttributeIdx;
	private Object[] attributeValues;
	
	public PGSQLOutput(PGConnection connection, CompositeType type) {
		this.connection = connection;
		this.type = type;
		this.attributeValues = new Object[type.getAttributes().size()];
	}

	public Object[] getAttributeValues() {
		return attributeValues;
	}

	void writeNextAttributeValue(Object val) throws SQLException {
		
		Attribute attr = type.getAttribute(currentAttributeIdx+1);
		if(attr == null) {
			throw new SQLException("invalid attribute access");
		}
		
		Class<?> targetType = mapSetType(attr.type);
		
		attributeValues[currentAttributeIdx++] = coerce(val, attr.type, targetType, Collections.<String,Class<?>>emptyMap(), connection);
	}

	@Override
	public void writeString(String x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeBoolean(boolean x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeByte(byte x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeShort(short x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeInt(int x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeLong(long x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeFloat(float x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeDouble(double x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeBigDecimal(BigDecimal x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeBytes(byte[] x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeDate(Date x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeTime(Time x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeTimestamp(Timestamp x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeCharacterStream(Reader x) throws SQLException {
		try {
			writeNextAttributeValue(CharStreams.toString(x));
		}
		catch(IOException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void writeAsciiStream(InputStream x) throws SQLException {
		try {
			writeNextAttributeValue(new String(ByteStreams.toByteArray(x), StandardCharsets.US_ASCII));
		}
		catch(IOException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void writeBinaryStream(InputStream x) throws SQLException {
		try {
			writeNextAttributeValue(ByteStreams.toByteArray(x));
		}
		catch(IOException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void writeArray(Array x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeURL(URL x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeObject(SQLData x) throws SQLException {		
		writeNextAttributeValue(x);
	}

	public void writeObject(Object x) throws SQLException {		
		writeNextAttributeValue(x);
	}

	@Override
	public void writeBlob(Blob x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeStruct(Struct x) throws SQLException {
		writeNextAttributeValue(x);
	}

	@Override
	public void writeSQLXML(SQLXML x) throws SQLException {
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void writeRowId(RowId x) throws SQLException {
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void writeRef(Ref x) throws SQLException {
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void writeClob(Clob x) throws SQLException {
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void writeNString(String x) throws SQLException {
		throw NOT_SUPPORTED;
	}

	@Override
	public void writeNClob(NClob x) throws SQLException {
		throw NOT_SUPPORTED;
	}

}
