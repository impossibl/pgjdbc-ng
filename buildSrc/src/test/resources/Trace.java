package com.impossibl.jdbc.spy;

import java.util.LinkedHashMap;
import java.util.stream.Collectors;

// Shell for compilation test
public class Trace {

  public static class Builder {

    public Builder(String type, String name) {
    }

    public Builder withParameter(String name, Object value) {
      return this;
    }

    public Builder returned(Object value) {
      return this;
    }

    public Builder returned() {
      return this;
    }

    public Builder threw(Throwable value) {
      return this;
    }

    public Trace build() {
      return new Trace();
    }

  }

  public Trace() {
  }

}
