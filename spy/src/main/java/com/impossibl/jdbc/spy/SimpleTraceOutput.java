package com.impossibl.jdbc.spy;

import java.io.IOException;
import java.io.Writer;

public class SimpleTraceOutput implements TraceOutput {

  public Writer out;

  public SimpleTraceOutput(Writer out) {
    this.out = out;
  }

  @Override
  public void trace(Trace trace) {
    try {
      this.out.append(trace.toString()).append("\n").flush();
    }
    catch (IOException ignored) {
    }
  }

}
