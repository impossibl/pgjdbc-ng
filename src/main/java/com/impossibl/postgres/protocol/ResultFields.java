package com.impossibl.postgres.protocol;

import com.impossibl.postgres.system.Context;

public class ResultFields {

  public static FieldFormat[] getResultFieldFormats(Context context, ResultField[] resultFields) {

    FieldFormat[] resultFieldFormats = new FieldFormat[resultFields.length];

    for (int idx = 0; idx < resultFieldFormats.length; ++idx) {
      ResultField resultField = resultFields[idx];
      resultField.setFormat(resultField.getTypeRef().getType(context).getResultFormat());
      resultFieldFormats[idx] = resultField.getFormat();
    }

    return resultFieldFormats;
  }

}
