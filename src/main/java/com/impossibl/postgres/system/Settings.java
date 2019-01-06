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
import java.util.Map;
import java.util.Properties;
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
   * Creates a bag of settings that recognizes any setting defined.
   */
  public Settings() {
    known = Setting.getAll();
  }

  /**
   * Creates a bag of settings that recognizes only settings from
   * the provided groups.
   *
   * @param groups Array of setting groups to recognize.
   */
  public Settings(Setting.Group... groups) {
    for (Setting.Group group : groups) {
      known.putAll(group.getAllSettings());
    }
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
   * @param <T> Type of the setting (inferred by {@code setting}
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
   * @param <T> Type of the setting (inferred by {@code setting}
   * @return Stored value of setting or null.
   */
  public <T> T getStored(Setting<T> setting) {
    String value = values.get(setting.getName());
    if (value != null) {
      return setting.fromString(value);
    }
    return null;
  }

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
   * Transfers all settings from given settings bag
   * into this instance.
   *
   * @param settings Settings to transfer
   */
  public void setAll(Settings settings) {
    settings.values.forEach(this::set);
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
      values.put(name, text);
      return;
    }

    // Validate setting value
    try {
      setting.fromString(text);
    }
    catch (Exception e) {
      logger.severe("Setting '" + name + "' to an invalid value: " + text);
    }

    // Map name to setting's primary name
    name = setting.getName();

    values.put(name, text);
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

}
