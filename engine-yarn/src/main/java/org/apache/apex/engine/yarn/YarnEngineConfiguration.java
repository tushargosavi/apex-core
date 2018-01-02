/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.yarn;

import org.apache.apex.engine.api.Configuration;
import org.apache.apex.engine.api.Settings;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class YarnEngineConfiguration implements Configuration
{
  YarnConfiguration conf;

  enum Type
  {
    ENVIRON, VAR, CONFIG, VALUE
  }

  private static final Table<Setting<?>, Type, Object> settings = HashBasedTable.create();

  static {
    settings.put(Settings.CONTAINER_ID, Type.ENVIRON, ApplicationConstants.Environment.CONTAINER_ID.key());
    settings.put(Settings.JAVA_HOME, Type.ENVIRON, ApplicationConstants.Environment.JAVA_HOME.key());
    settings.put(Settings.USER, Type.ENVIRON, ApplicationConstants.Environment.USER.key());

    settings.put(Settings.LOG_DIR_EXPANSION, Type.VAR, ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    settings.put(Settings.JAVA_HOME, Type.VAR, ApplicationConstants.Environment.JAVA_HOME.$());
    settings.put(Settings.PWD, Type.VAR, ApplicationConstants.Environment.PWD.$());

    settings.put(Settings.APPLICATION_CLASSPATH, Type.CONFIG, YarnConfiguration.YARN_APPLICATION_CLASSPATH);

    settings.put(Settings.DEFAULT_CONTAINER_TEMP_DIR, Type.VALUE, YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    settings.put(Settings.DEFAULT_APPLICATION_CLASSPATH, Type.VALUE, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
  }

  public YarnEngineConfiguration(YarnConfiguration conf)
  {
    this.conf = conf;
  }

  @Override
  public <V> V get(Setting<V> setting)
  {
    Object name = settings.get(setting, Type.ENVIRON);
    if (name != null) {
      return (V)System.getenv((String)name);
    }
    name = settings.get(setting, Type.CONFIG);
    if (name != null) {
      return (V)conf.get((String)name);
    }
    return (V)settings.get(setting, Type.VALUE);
  }

  @Override
  public String getVar(Setting setting)
  {
    return (String)settings.get(setting, Type.VAR);
  }

  @Override
  public org.apache.hadoop.conf.Configuration getNativeConfig()
  {
    return conf;
  }
}
