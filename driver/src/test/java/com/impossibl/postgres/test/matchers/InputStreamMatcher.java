package com.impossibl.postgres.test.matchers;

import com.impossibl.postgres.system.procs.Bytes;

import static com.impossibl.postgres.utils.guava.ByteStreams.limit;
import static com.impossibl.postgres.utils.guava.ByteStreams.toByteArray;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;


public class InputStreamMatcher extends BaseMatcher<InputStream> {

  private InputStream expected;

  private InputStreamMatcher(InputStream expected) {
    this.expected = expected;
  }

  @Override
  public boolean matches(Object item) {
    InputStream actual = (InputStream) item;
    try {
      return Arrays.equals(toByteArray(expected), toByteArray(actual));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void describeTo(Description description) {
    try {
      byte[] data = toByteArray(limit(expected, 20));
      description.appendText("InputStream (" + data.length + ")").appendValue(Bytes.encodeHex(data));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static InputStreamMatcher contentEquals(InputStream expected) {
    return new InputStreamMatcher(expected);
  }

}
