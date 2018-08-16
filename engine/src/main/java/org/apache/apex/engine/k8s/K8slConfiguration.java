/**
 * Copyright (c) 2012-2018 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.k8s;

import org.apache.apex.engine.api.Configuration;
import org.apache.hadoop.util.Shell;

/**
 * Created by sergey on 2/2/18.
 */
public class K8slConfiguration implements Configuration
{
  org.apache.hadoop.conf.Configuration configuration;

  public K8slConfiguration()
  {
    configuration = new org.apache.hadoop.conf.Configuration();
  }

  @Override
  public <V> V get(Setting<V> setting)
  {
    Object value = null;
    if (setting == Settings.CONTAINER_ID) {
      value = "CONTAINER_ID";
    } else if (setting == Settings.JAVA_HOME) {
      value = "JAVA_HOME";
    } else if (setting == Settings.USER) {
      value = "USER";
    } else if (setting == Settings.APPLICATION_CLASSPATH) {
      value = configuration.get("yarn.application.classpath");
    } else if (setting == Settings.DEFAULT_CONTAINER_TEMP_DIR) {
      value = "./tmp";
    } else if (setting == Settings.DEFAULT_APPLICATION_CLASSPATH) {
      value = new String[] {getEvalVarExp("HADOOP_CONF_DIR")};
    }
    return (V)value;
  }

  @Override
  public String getVar(Setting<?> setting)
  {
    if (setting == Settings.LOG_DIR_EXPANSION) {
      return "<LOG_DIR>";
    } else if (setting == Settings.JAVA_HOME) {
      return getEvalVarExp("JAVA_HOME");
    } else if (setting == Settings.PWD) {
      return getEvalVarExp("PWD");
    }
    return null;
  }

  @Override
  public org.apache.hadoop.conf.Configuration getNativeConfig()
  {
    return configuration;
  }

  private String getEvalVarExp(String var)
  {
    if (Shell.WINDOWS) {
      return "%" + var + "%";
    } else {
      return "$" + var;
    }
  }
}
