package com.impossibl.postgres.protocol;

import static com.impossibl.postgres.protocol.FieldFormat.Binary;
import static com.impossibl.postgres.protocol.FieldFormat.Text;

public class FieldFormats {

  public static final FieldFormat[] REQUEST_ALL_TEXT = new FieldFormat[] {Text};
  public static final FieldFormat[] REQUEST_ALL_BINARY = new FieldFormat[] {Binary};

}
