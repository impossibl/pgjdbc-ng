package com.impossibl.postgres.test.extensions;

import com.impossibl.postgres.test.annotations.Random;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class RandomProvider implements ParameterResolver {

  private static final Namespace NS = Namespace.create("random");

  @Override
  public boolean supportsParameter(ParameterContext paramCtx, ExtensionContext extensionContext) throws ParameterResolutionException {

    if (!paramCtx.isAnnotated(Random.class)) return false;

    Class<?> paramType = paramCtx.getParameter().getType();

    return paramType == IntStream.class || paramType == InputStream.class || paramType == Reader.class ||
        paramType == byte[].class ||
        paramType == Boolean.class || paramType == Boolean.TYPE ||
        paramType == Integer.class || paramType == Integer.TYPE ||
        paramType == Long.class || paramType == Long.TYPE ||
        paramType == Float.class || paramType == Float.TYPE ||
        paramType == Double.class || paramType == Double.TYPE;
  }

  @Override
  public Object resolveParameter(ParameterContext paramCtx, ExtensionContext extCtx) throws ParameterResolutionException {

    Random randomAnn = paramCtx.findAnnotation(Random.class).orElse(null);
    if (randomAnn == null) return null;

    java.util.Random random = (java.util.Random) extCtx.getStore(NS)
        .getOrComputeIfAbsent("provider", key -> new java.util.Random());

    Class<?> paramType = paramCtx.getParameter().getType();

    if (paramType == byte[].class) {
      int[] values = random.ints(randomAnn.size(), randomAnn.origin(), randomAnn.bound()).toArray();
      byte[] data = new byte[randomAnn.size()];
      for (int c=0; c < data.length; ++c) {
        data[c] = (byte) values[c];
      }
      return data;
    }

    if (paramType == InputStream.class) {
      if (randomAnn.codepoints()) {
        int[] values = randomCodePoints(random).limit(randomAnn.size()).toArray();
        String text = new String(values, 0, values.length);
        return new ByteArrayInputStream(text.getBytes(UTF_8));
      }
      else {
        int[] values = random.ints(randomAnn.size(), randomAnn.origin(), randomAnn.bound()).toArray();
        byte[] data = new byte[randomAnn.size()];
        for (int c=0; c < data.length; ++c) {
          data[c] = (byte) values[c];
        }
        return new ByteArrayInputStream(data);
      }
    }

    if (paramType == Reader.class) {
      int[] values = randomCodePoints(random).limit(randomAnn.size()).toArray();
      return new StringReader(new String(values, 0, values.length));
    }

    if (paramType == String.class) {
      int[] values = randomCodePoints(random).limit(randomAnn.size()).toArray();
      return new String(values, 0, values.length);
    }

    if (paramType == Boolean.class || paramType == Boolean.TYPE) {
      return random.nextBoolean();
    }

    if (paramType == Integer.class || paramType == Integer.TYPE) {
      return random.nextInt(randomAnn.bound());
    }

    if (paramType == Long.class || paramType == Long.TYPE) {
      return random.nextLong();
    }

    if (paramType == Float.class || paramType == Float.TYPE) {
      return random.nextFloat();
    }

    if (paramType == Double.class || paramType == Double.TYPE) {
      return random.nextDouble();
    }

    if (paramType == IntStream.class) {
      return random.ints(randomAnn.size(), randomAnn.origin(), randomAnn.bound());
    }

    return null;
  }

  private IntStream randomCodePoints(java.util.Random random) {
    return random.ints(0, 0xfffe)
        .filter(cp -> Character.isDefined(cp) && !Character.isSurrogate((char) cp) && Character.getType(cp) != Character.PRIVATE_USE);
  }

}
