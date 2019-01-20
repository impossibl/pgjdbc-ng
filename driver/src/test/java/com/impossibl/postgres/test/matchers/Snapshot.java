package com.impossibl.postgres.test.matchers;

import java.util.List;

public interface Snapshot<T> {

  List<T> take();

}
