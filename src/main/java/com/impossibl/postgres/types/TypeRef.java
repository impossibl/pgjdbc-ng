package com.impossibl.postgres.types;

import com.impossibl.postgres.system.Context;

public interface TypeRef {

  Type getType(Context context);

}
