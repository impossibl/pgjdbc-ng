package com.impossibl.jdbc.spy;

import java.util.LinkedHashMap;
import java.util.stream.Collectors;

public class Trace {

  public static class Builder {

    private String type;
    private String method;
    private LinkedHashMap<String, Object> parameters;
    private Object result;
    private boolean isVoid;
    private Throwable cause;

    public Builder(String type, String name) {
      this.type = type;
      this.method = name;
      this.parameters = new LinkedHashMap<>();
    }

    public Builder withParameter(String name, Object value) {
      parameters.put(name, value);
      return this;
    }

    public Builder returned(Object value) {
      result = value;
      isVoid = false;
      return this;
    }

    public Builder returned() {
      result = null;
      isVoid = true;
      return this;
    }

    public Builder threw(Throwable value) {
      cause = value;
      return this;
    }

    public Trace build() {
      return new Trace(type, method, parameters, isVoid, result, cause);
    }

  }

  private String type;
  private String method;
  private LinkedHashMap<String, Object> parameters;
  private boolean isVoid;
  private Object result;
  private Throwable cause;

  public Trace(String type, String method, LinkedHashMap<String, Object> parameters, boolean isVoid, Object result, Throwable cause) {
    this.type = type;
    this.method = method;
    this.parameters = parameters;
    this.isVoid = isVoid;
    this.result = result;
    this.cause = cause;
  }

  @Override
  public String toString() {
    String res =
        type + "." + method + "(" +
            parameters.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(", ")) +
            ")";
    if (cause == null) {
      res += " returned(" + (isVoid ? "" : result) + ")";
    }
    else {
      res += " threw " + cause.getClass().getSimpleName() + "(" + cause.getMessage() + ")";
    }
    return res;
  }

}
