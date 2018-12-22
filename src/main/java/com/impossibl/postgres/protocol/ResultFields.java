package com.impossibl.postgres.protocol;

import java.util.function.Function;

public class ResultFields {

  public static void transformTypes(ResultField[] fields, Function<TypeRef, TypeRef> consumer) {
    for (ResultField field : fields) {
      field.setTypeRef(consumer.apply(field.getTypeRef()));
    }
  }

}
