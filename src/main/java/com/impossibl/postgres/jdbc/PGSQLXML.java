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
package com.impossibl.postgres.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stax.StAXResult;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.Node;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;



public class PGSQLXML implements SQLXML {

  class OutputStream extends ByteArrayOutputStream {

    @Override
    public void flush() throws IOException {
      PGSQLXML.this.data = buf;
      PGSQLXML.this.dataLen = count;
      PGSQLXML.this.initialized = true;
    }

  }


  class InternalDOMResult extends DOMResult {

    @Override
    public void setNode(Node node) {
      super.setNode(node);

      try {
        TransformerFactory factory = TransformerFactory.newInstance();
        Transformer transformer = factory.newTransformer();
        DOMSource domSource = new DOMSource(node);

        Writer writer = new OutputStreamWriter(new OutputStream(), connection.getCharset());
        StreamResult streamResult = new StreamResult(writer);

        transformer.transform(domSource, streamResult);
      }
      catch (TransformerFactoryConfigurationError | TransformerException e) {
        //Ignore exceptions until later
        PGSQLXML.this.data = null;
        PGSQLXML.this.initialized = true;
      }
    }

  }


  private PGConnectionImpl connection;
  private byte[] data;
  private int dataLen;
  private boolean initialized;


  public PGSQLXML(PGConnectionImpl conn) {
    this(conn, null, false);
  }

  public PGSQLXML(PGConnectionImpl conn, byte[] data) {
    this(conn, data, true);
  }

  private PGSQLXML(PGConnectionImpl connection, byte[] data, boolean initialized) {
    this.connection = connection;
    this.data = data;
    this.dataLen = data != null ? data.length : -1;
    this.initialized = initialized;
  }

  byte[] getData() {

    if (data == null) {
      return null;
    }

    return Arrays.copyOf(data, dataLen);
  }

  private void checkFreed() throws SQLException {

    if (connection == null) {
      throw new SQLException("SQLXML object has already been freed");
    }
  }

  private void checkReadable() throws SQLException {

    if (!initialized) {
      throw new SQLException("SQLXML object has not been initialized");
    }
  }

  private void checkWritable() throws SQLException {

    if (initialized) {
      throw new SQLException("SQLXML object has already been initialized");
    }
  }

  public void free() {
    connection = null;
    data = null;
  }

  public InputStream getBinaryStream() throws SQLException {
    checkFreed();
    checkReadable();

    if (data == null)
      return null;

    return new ByteArrayInputStream(data, 0, dataLen);
  }

  public Reader getCharacterStream() throws SQLException {
    checkFreed();
    checkReadable();

    if (data == null)
      return null;

    return new InputStreamReader(new ByteArrayInputStream(data, 0, dataLen), connection.getCharset());
  }

  public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
    checkFreed();
    checkReadable();

    if (data == null)
      return null;

    try {

      if (sourceClass == null || DOMSource.class.equals(sourceClass)) {

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        DocumentBuilder builder = factory.newDocumentBuilder();
        builder.setErrorHandler(new ErrorHandler() {

          @Override
          public void warning(SAXParseException exception) throws SAXException {
          }

          @Override
          public void error(SAXParseException exception) throws SAXException {
          }

          @Override
          public void fatalError(SAXParseException exception) throws SAXException {
          }
        });

        InputSource input = new InputSource(getCharacterStream());

        return sourceClass.cast(new DOMSource(builder.parse(input)));
      }
      else if (SAXSource.class.equals(sourceClass)) {

        InputSource is = new InputSource(getCharacterStream());

        return sourceClass.cast(new SAXSource(is));
      }
      else if (StreamSource.class.equals(sourceClass)) {

        return sourceClass.cast(new StreamSource(getCharacterStream()));
      }
      else if (StAXSource.class.equals(sourceClass)) {

        XMLInputFactory xif = XMLInputFactory.newInstance();

        XMLStreamReader xsr = xif.createXMLStreamReader(getCharacterStream());

        return sourceClass.cast(new StAXSource(xsr));
      }

    }
    catch (XMLStreamException | SAXException | IOException | ParserConfigurationException e) {
      throw new SQLException("Error initializing XML source");
    }

    throw new SQLException("Unsupported XML Source class" + sourceClass.getName());
  }

  public String getString() throws SQLException {
    checkFreed();
    checkReadable();

    if (data == null)
      return null;

    return new String(data, 0, dataLen, connection.getCharset());
  }

  public OutputStream setBinaryStream() throws SQLException {
    checkFreed();
    checkWritable();

    return new OutputStream();
  }

  public Writer setCharacterStream() throws SQLException {
    checkFreed();
    checkWritable();

    return new OutputStreamWriter(new OutputStream(), connection.getCharset());
  }

  public <T extends Result> T setResult(Class<T> resultClassIn) throws SQLException {
    checkFreed();
    checkWritable();

    @SuppressWarnings("unchecked")
    Class<T> resultClass = (Class<T>) DOMResult.class;
    if (resultClassIn != null) {
      resultClass = resultClassIn;
    }

    if (DOMResult.class.equals(resultClass)) {

      return resultClass.cast(new InternalDOMResult());
    }
    else if (SAXResult.class.equals(resultClass)) {

      try {

        SAXTransformerFactory transformerFactory = (SAXTransformerFactory) SAXTransformerFactory.newInstance();
        TransformerHandler transformerHandler = transformerFactory.newTransformerHandler();

        Writer writer = setCharacterStream();
        transformerHandler.setResult(new StreamResult(writer));

        return resultClass.cast(new SAXResult(transformerHandler));
      }
      catch (TransformerException te) {

        throw new SQLException("Error initializing SAXResult");
      }
    }
    else if (StreamResult.class.equals(resultClass)) {

      Writer writer = setCharacterStream();

      return resultClass.cast(new StreamResult(writer));
    }
    else if (StAXResult.class.equals(resultClass)) {

      Writer writer = setCharacterStream();

      try {

        XMLOutputFactory xof = XMLOutputFactory.newInstance();
        XMLStreamWriter xsw = xof.createXMLStreamWriter(writer);

        return resultClass.cast(new StAXResult(xsw));
      }
      catch (XMLStreamException xse) {

        throw new SQLException("Error initializing StAXResult");
      }
    }

    throw new SQLException("Unsupported XML Result class" + resultClass.getName());
  }

  public void setString(String value) throws SQLException {
    checkFreed();
    checkWritable();

    initialized = true;

    if (value != null) {
      data = value.getBytes(connection.getCharset());
    }
    else {
      data = null;
    }

  }

}
