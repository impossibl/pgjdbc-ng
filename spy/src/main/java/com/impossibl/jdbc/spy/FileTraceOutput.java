package com.impossibl.jdbc.spy;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.util.Properties;

public class FileTraceOutput extends SimpleTraceOutput {

  public FileTraceOutput(Properties info) throws SQLException {
    super(getWriterForInfo(info));
  }

  private static Writer getWriterForInfo(Properties info) throws SQLException {

    String fileName = info.getProperty("spy.tracer.file", System.getProperty("spy.tracer.file", "@out"));
    info.remove("spy.tracer.file");

    switch (fileName) {
      case "@out":
        return new OutputStreamWriter(System.out);

      case "@err":
        return new OutputStreamWriter(System.err);

      default:
        try {
          return new OutputStreamWriter(new FileOutputStream(fileName));
        }
        catch (IOException e) {
          throw new SQLException("SPY: Unable to initialize tracer to file: " + fileName);
        }
    }

  }

}
