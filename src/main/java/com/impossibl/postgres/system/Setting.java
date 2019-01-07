/**
 * Copyright (c) 2013, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.impossibl.postgres.system;

import static com.impossibl.postgres.utils.StringTransforms.toLowerCamelCase;
import static com.impossibl.postgres.utils.guava.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Completely defined setting that can be transformed to/from text, use alternate
 * names, carries a description and optional default value.
 *
 * Each setting requires a "primary" name (available via {@link #getName()}. This name
 * is used when storing settings and when displaying information about the setting.
 *
 * Each setting is allowed to have alternate names that can be used when searching a source
 * for an setting's value. For example, the {@link #getSystem()} method searches for a value
 * in the system properties by looking up the primary name & then alternate names in turn
 * until it finds a non-null value.
 *
 * All settings are required to have unique names (including their alternate names); it
 * is enforced during instantiation and will throw an exception is duplicates are found.
 *
 * A global map of setting names to setting instances is provided via {@link #getAll()};
 *
 * @param <T> Type of the setting
 */
public class Setting<T> {

  private static final Logger logger = Logger.getLogger(Setting.class.getName());

  /**
   * Setting group.
   *
   * Should be defined as constant then use the {@link #add} methods
   * to add setting definitions to the group.
   *
   * <code>
   *   static final Setting.Group MYGROUP = new Setting.Group("MySettings");
   *
   *   static final MY_SETTING = MYGROUP.add(...);
   * </code>
   */
  public static class Group {

    private static final Map<String, Group> ALL = new HashMap<>();

    /**
     * Name base map of all defined setting groups
     * @return Map of defined setting groups
     */
    public static Map<String, Group> getAll() {
      return Collections.unmodifiableMap(ALL);
    }

    private String name;
    private String description;
    private Map<String, Setting<?>> all = new HashMap<>();

    public Group(String name, String description) {
      this.name = name;
      this.description = description;
      synchronized (Group.class) {
        ALL.putIfAbsent(name, this);
      }
    }

    /**
     * Get name of group
     *
     * @return Name of group
     */
    public String getName() {
      return name;
    }

    /**
     * Geta a names based map of all settings in the group
     *
     * @return Map of all settings.
     */
    public Map<String, Setting<?>> getAllSettings() {
      return Collections.unmodifiableMap(all);
    }

    /**
     * Add a previously defined {@link Setting} instance to this setting group.
     */
    public <T> Setting<T> add(Setting<T> setting) {
      for (String name : setting.getNames()) {
        all.put(name, setting);
      }
      return setting;
    }

    /**
     * Add a boolean setting to the group.
     *
     * @param description Description of the setting (can be anything and any length)
     * @param defaultBool Default value of the setting or {@code null}.
     * @param names List of names associated with the setting; first in list is primary name.
     * @return Defined setting instance
     */
    public Setting<Boolean> add(String description, Boolean defaultBool, String... names) {
      return new Setting<>(this, description, Boolean.class, defaultBool, Boolean::parseBoolean, Object::toString, names);
    }

    /**
     * Add an integer setting to the group.
     *
     * @param description Description of the setting (can be anything and any length)
     * @param defaultInt Default value of the setting or {@code null}.
     * @param names List of names associated with the setting; first in list is primary name.
     * @return Defined setting instance
     */
    public Setting<Integer> add(String description, Integer defaultInt, String... names) {
      return new Setting<>(this, description, Integer.class, defaultInt, Integer::parseInt, Object::toString, names);
    }

    /**
     * Add a string setting to the group.
     *
     * @param description Description of the setting (can be anything and any length)
     * @param defaultString Default value of the setting or {@code null}.
     * @param names List of names associated with the setting; first in list is primary name.
     * @return Defined setting instance
     */
    public Setting<String> add(String description, String defaultString, String... names) {
      return new Setting<>(this, description, String.class, defaultString, s -> s, s -> s, names);
    }

    /**
     * Adds an enum setting to the group.
     *
     * The {@code transform} parameter is used to alter the string value before the
     * {@link Enum#valueOf(Class, String)} method is used to produce a value from string. For example,
     * a setting might use {@link String#toUpperCase()} to ensure the string is in the same case as the
     * enumeration constant.
     *
     * @param description Description of the setting (can be anything and any length)
     * @param defaultEnum Default value of the setting; cannot be {@code null}.
     * @param names List of names associated with the setting; first in list is primary name.
     * @return Defined setting instance
     */
    public <U extends Enum<U>> Setting<U> add(String description, U defaultEnum, Function<String, String> fromTransform, Function<String, String> toTransform, String... names) {
      Class<U> type = defaultEnum.getDeclaringClass();
      Setting.Converter<U> fromString = str -> type.cast((Object) Enum.valueOf(type.asSubclass(Enum.class), fromTransform.apply(str)));
      Function<U, String> toString = e -> toTransform.apply(e.name());

      return new Setting<>(this, description, defaultEnum.getDeclaringClass(), defaultEnum, fromString, toString, names);
    }

    /**
     * Add a {@link Class} setting to the group.
     *
     * The class is looked up using {@link Class#forName(String)} and store as the result of
     * {@link Class#getName()}.
     *
     * @param description Description of the setting (can be anything and any length)
     * @param defaultClass Default value of the setting or {@code null}.
     * @param names List of names associated with the setting; first in list is primary name.
     * @return Defined setting instance
     */
    public Setting<Class> add(String description, Class defaultClass, String... names) {
      return new Setting<>(this, description, Class.class, defaultClass, Class::forName, Class::getName, names);
    }

    /**
     * Add a generic typed setting to the group.
     *
     * Allows adding types of settings not already handled by another {@code add} method.
     *
     * @param description Description of the setting (can be anything and any length)
     * @param type Class of the setting.
     * @param defaultValue Default value of the setting or {@code null}.
     * @param fromString Function to convert from a {@link String} to a setting's native type {@link U}.
     * @param toString Function to convert from setting's native type {@link U} to {@link String}.
     * @param names List of names associated with the setting; first in list is primary name.
     * @param <U> Type of the setting.
     * @return Defined setting instance
     */
    public <U> Setting<U> add(String description, Class<U> type, U defaultValue, Setting.Converter<U> fromString, Function<U, String> toString, String... names) {
      return new Setting<>(this, description, type, defaultValue, fromString, toString, names);
    }

    /**
     * Add a generic typed setting to the group with a dynamic default value.
     *
     * @param description Description of the setting (can be anything and any length)
     * @param type Class of the setting.
     * @param defaultValueSupplier Supplier of the default value of the setting (as text) or {@code null}.
     * @param fromString Function to convert from a {@link String} to a setting's native type {@link U}.
     * @param toString Function to convert from setting's native type {@link U} to {@link String}.
     * @param names List of names associated with the setting; first in list is primary name.
     * @param <U> Type of the setting.
     * @return Defined setting instance
     */
    public <U> Setting<U> add(String description, Class<U> type, Supplier<String> defaultValueSupplier, Setting.Converter<U> fromString, Function<U, String> toString, String... names) {
      return new Setting<>(this, description, type, defaultValueSupplier, fromString, toString, names);
    }

    @Override
    public String toString() {
      return name;
    }

  }




  public interface Converter<U> {

    U convert(String string) throws Exception;

  }

  private static final Map<String, Setting<?>> ALL = new HashMap<>();

  /**
   * Get a name based map of all defined settings.
   *
   * All settings mapped by all names (primary & alternates) are
   * provided in the map.
   *
   * @return Map of all defined settings.
   */
  public static Map<String, Setting<?>> getAll() {
    return Collections.unmodifiableMap(ALL);
  }

  /**
   * Prefix used when looking up values via {@link System#getProperty(String)}
   */
  private static final String SYSTEM_PREFIX = "pgjdbc.";

  private Group group;
  private String[] names;
  private Class<? extends T> type;
  private T staticDefaultValue;
  private Supplier<String> dynamicDefaultSupplier;
  private Converter<T> fromString;
  private Function<T, String> toString;
  private String description;

  private Setting(Group group, String description, Class<T> type, T defaultValue, Converter<T> fromString, Function<T, String> toString, String[] names) {
    this(group, description, type, fromString, toString, names);
    this.staticDefaultValue = defaultValue;
  }

  private Setting(Group group, String description, Class<T> type, Supplier<String> defaultSupplier, Converter<T> fromString, Function<T, String> toString, String[] names) {
    this(group, description, type, fromString, toString, names);
    this.dynamicDefaultSupplier = defaultSupplier;
  }

  private Setting(Group group, String description, Class<T> type, Converter<T> fromString, Function<T, String> toString, String[] names) {
    checkArgument(names.length > 0);
    checkArgument(isSimpleNameFormat(names[0]));
    this.group = group;
    this.names = names;
    this.type = type;
    this.fromString = fromString;
    this.toString = toString;
    this.description = description;
    synchronized (Setting.class) {
      addAll(ALL, this);
      addAll(group.all, this);
    }
  }

  private static final Pattern SIMPLE_NAME_PATTERN = Pattern.compile("(?:[a-z][a-z0-9\\-_]+)(?:\\.[a-z0-9][a-z0-9\\-_]+)*");

  /**
   * Validate primary name as being all lowercase and compatible with generating
   * a JavaBean property name (doesn't start with a number, dash or dot).
   */
  private static boolean isSimpleNameFormat(String name) {
    return SIMPLE_NAME_PATTERN.matcher(name).matches();
  }

  private static void addAll(Map<String, Setting<?>> settings, Setting<?> instance) {
    for (String name : instance.names) {
      if (settings.containsKey(name)) {
        throw new IllegalStateException("Setting with name '" + name + "' already exists");
      }
      settings.put(name, instance);
    }
  }

  public Group getGroup() {
    return group;
  }

  public String getName() {
    return names[0];
  }

  public String getBeanPropertyName() {
    return toLowerCamelCase(names[0]);
  }

  public String[] getNames() {
    return names;
  }

  public Class<? extends T> getType() {
    return type;
  }

  public T getDefault() {
    if (dynamicDefaultSupplier != null) {
      String value = dynamicDefaultSupplier.get();
      if (value == null) return null;
      return fromString(value);
    }
    return staticDefaultValue;
  }

  public String getDefaultText() {
    if (dynamicDefaultSupplier != null) {
      return dynamicDefaultSupplier.get();
    }
    if (staticDefaultValue != null) {
      return toString(staticDefaultValue);
    }
    return null;
  }

  public String getDescription() {
    return description;
  }

  /**
   * Convert a string to the setting's native type.
   *
   * @param value String value to parse.
   * @return Value in the settings native type.
   * @throws IllegalArgumentException If the value cannot be parsed.
   */
  public T fromString(String value) {
    try {
      return fromString.convert(value);
    }
    catch (Exception e) {
      throw new IllegalArgumentException("Unable to parse setting \"" + getName() + "\" from '" + value + "'");
    }
  }

  /**
   * Convert a native type to a string value.
   *
   * @param value Native value to convert into text.
   * @return Value in text form.
   */
  public String toString(T value) {
    return toString.apply(value);
  }

  /**
   * Looks up the setting in system properties.
   *
   * This method tries all names (primary & alternates) in the
   * order in which they were defined and returns the first
   * non-null value.
   *
   * If no value is found the settings {@link #getDefault() default}
   * value is returned.
   *
   * @return System property value of the setting or its default value.
   */
  public T getSystem() {
    for (String name : names) {
      String value = System.getProperty(SYSTEM_PREFIX + name);
      if (value != null) {
        return fromString(value);
      }
    }
    return getDefault();
  }

  public T get(Properties properties) {
    for (String name : names) {
      String value = properties.getProperty(name);
      if (value != null) {
        return fromString(value);
      }
    }
    return getDefault();
  }

  @Override
  public String toString() {
    String defaultValue = getDefaultText();
    return group + ": " + Arrays.toString(names) + " (" + type + ") = " + (defaultValue != null ? defaultValue : "null") + " : " + description;
  }

  /**
   * Marker interface that works with ServiceLoader to ensure
   * that while initializing the {@link Setting}, that all classes
   * defining settings & groups are initialized.
   *
   * After initialization, the settings maps can be frozen.
   */
  public interface Provider {
  }

  /**
   * Use service loader to ensure all settings are defined while defining this class
   */
  static {
    for (Provider provider : ServiceLoader.load(Provider.class)) {
      logger.config("Loaded settings from " + provider.getClass().getName());
    }
  }


}
