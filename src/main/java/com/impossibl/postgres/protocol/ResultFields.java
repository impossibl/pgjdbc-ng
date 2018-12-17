package com.impossibl.postgres.protocol;

public class ResultFields {

  public static FieldFormat[] getResultFieldFormats(ResultField[] resultFields) {

    FieldFormat[] resultFieldFormats = new FieldFormat[resultFields.length];

    for (int idx = 0; idx < resultFieldFormats.length; ++idx) {
      ResultField resultField = resultFields[idx];
      resultField.setFormat(resultField.getTypeRef().getType().getResultFormat());
      resultFieldFormats[idx] = resultField.getFormat();
    }

    return resultFieldFormats;
  }

}
