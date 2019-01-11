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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;


/**
 * Managed bag of textual settings that can be read & updated.
 *
 * The values for each setting are stored as text and converted as necessary.
 *
 * The settings are store via their primary name (see {@link Setting}). When
 * setting values via one of a setting's alternate names the name is mapped
 * back to the primary before storage.
 *
 * Values set via raw text are validated using {@link Setting#fromString(String)}
 * before they are stored; an error is logged if validation fails.
 *
 * If a setting name is unrecognized, a warning is logged before storing the value.
 *
 */
public class Settings {

  private static final Logger logger = Logger.getLogger(Settings.class.getName());


  private Map<String, Setting<?>> known = new HashMap<>();
  private Map<String, String> values = new HashMap<>();

  /**
   * Creates a bag of settings that recognizes any global setting defined.
   */
  public Settings() {
    this(SettingGlobal.getAllGroups());
  }

  /**
   * Creates a bag of settings that recognizes only settings from
   * the provided groups.
   *
   * @param groups Array of setting groups to recognize.
   */
  public Settings(Setting.Group... groups) {
    for (Setting.Group group : groups) {
      known.putAll(group.getAllNamedSettings());
    }
  }

  public Settings duplicateKnowingAll() {
    Settings copy = new Settings();
    copy.values = new HashMap<>(values);
    return copy;
  }

  public Settings duplicateKnowing(Setting.Group... groups) {
    Settings copy = new Settings(groups);
    copy.values = new HashMap<>(values);
    return copy;
  }

  /**
   * Retrieves a list of settings known to this instance.
   *
   * @return Set of all known settings.
   */
  public Set<Setting<?>> knownSet() {
    return new HashSet<>(known.values());
  }

  /**
   * Check if settings has a stored value stored
   * for the  setting.
   *
   * @param setting Setting to check status of.
   * @return {@code true} if a value is stored for {@code setting}, {@code false} otherwise.
   */
  public boolean hasStoredValue(Setting<?> setting) {
    return values.containsKey(setting.getName());
  }

  /**
   * Retrieve a boolean value from a setting.
   *
   * If no value is set, and there is no default for the setting,
   * false is returned.
   *
   * @param setting Boolean setting to retrieve.
   * @return Stored value of setting, its default value or {@code false}.
   */
  public boolean enabled(Setting<Boolean> setting) {
    Boolean value = get(setting);
    return value != null && value;
  }

  /**
   * Retrieve a value for a setting.
   *
   * @param setting Setting to retrieve.
   * @param <T> Type of the setting (inferred by {@code setting})
   * @return Stored value of setting or its default value.
   */
  public <T> T get(Setting<T> setting) {
    String value = values.get(setting.getName());
    if (value != null) {
      return setting.fromString(value);
    }
    return setting.getDefault();
  }

  /**
   * Retrieve a stored value for the setting, returning null
   * if no value was explicitly stored.
   *
   * @param setting Setting to retrieve.
   * @param <T> Type of the setting (inferred by {@code setting})
   * @return Stored value of setting or null.
   */
  public <T> T getStored(Setting<T> setting) {
    String value = values.get(setting.getName());
    if (value != null) {
      return setting.fromString(value);
    }
    return null;
  }

  /**
   * Retrieve text value for a setting.
   *
   * @param setting Setting to retrieve.
   * @return Stored text value of setting or its default value.
   */
  public String getText(Setting<?> setting) {
    String value = values.get(setting.getName());
    if (value != null) return value;
    return setting.getDefaultText();
  }

  /**
   * Sets a value for the specified setting.
   *
   * @param setting Setting to set.
   * @param value Native value to set.
   * @param <T> Type of the setting (inferred by {@code setting})
   */
  public <T> void set(Setting<T> setting, T value) {
    if (!known.containsKey(setting.getName())) {
      logger.warning("Applying unknown setting: " + setting.getName());
    }
    if (value == null) {
      values.remove(setting.getName());
    }
    else {
      values.put(setting.getName(), setting.toString(value));
    }
  }

  /**
   * Set text value for a setting.
   *
   * An error is logged if {@code value} cannot
   * be validated.
   *
   * @param setting Setting to set.
   * @param value Text value for setting.
   */
  public void setText(Setting<?> setting, String value) {

    // Validate setting value
    try {
      setting.fromString(value);
    }
    catch (Exception e) {
      logger.severe("Setting '" + value + "' to an invalid value: " + value);
    }

    if (value == null) {
      values.remove(setting.getName());
    }
    else {
      values.put(setting.getName(), value);
    }
  }

  /**
   * Sets a setting via string name and text value.
   *
   * If the name provided does not match any defined primary or
   * alternate name know to the settings bag a warning is logged.
   *
   * If the value provided does cannot be validated via the associated
   * setting's {@link Setting#fromString(String)} method, an error is
   * logged.
   *
   * @param name Name of the setting to store
   * @param text Value of the store to store
   */
  public void set(String name, String text) {
    Setting<?> setting = known.get(name);
    if (setting == null) {
      logger.warning("Applying unknown setting: " + name);
      if (text == null) {
        values.remove(name);
      }
      else {
        values.put(name, text);
      }
      return;
    }

    setText(setting, text);
  }

  /**
   * Transfers all settings from given settings bag
   * into this instance.
   *
   * @param settings Settings to transfer
   */
  public void setAll(Settings settings) {
    settings.values.forEach(this::set);
  }

  /**
   * Sets all values from the given map as settings via
   * the {@link #set(String, String)} method. All validation
   * applies.
   *
   * @param settings Map of values to apply as settings.
   */
  public void setAll(Map<String, String> settings) {
    settings.forEach(this::set);
  }

  /**
   * Sets all properties from the given instance as settings via
   * the {@link #set(String, String)} method. All validation
   * applies.
   *
   * @param properties Properties to apply as settings.
   */
  public void setAll(Properties properties) {
    for (String propertyName : properties.stringPropertyNames()) {
      set(propertyName, properties.getProperty(propertyName));
    }
  }

  /**
   * Remove any stored value associated with the setting.
   *
   * @param setting Setting to remove
   */
  public void unset(Setting<?> setting) {
    set(setting, null);
  }

  /**
   * Remove any stored value associated with the named setting.
   *
   * @param name Name of setting to remove
   */
  public void unset(String name) {
    set(name, null);
  }

  public Setting<?> mapUnknownSetting(Setting<?> setting) {
    for (String name : setting.getNames()) {
      Setting<?> mapped = known.get(name);
      if (mapped != null) {
        return mapped;
      }
    }
    return null;
  }

  public Properties asProperties() {
    Properties properties = new Properties();
    values.forEach(properties::setProperty);
    return properties;
  }

  public Settings addMappedUnknownSetting(Setting<?> setting, Properties to) {

    setting = mapUnknownSetting(setting);
    if (setting == null) return this;

    String text = getText(setting);
    if (text != null) {
      to.setProperty(setting.getName(), text);
    }

    return this;
  }

}
