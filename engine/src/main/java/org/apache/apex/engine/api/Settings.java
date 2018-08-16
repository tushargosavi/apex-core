/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.engine.api;

import org.apache.apex.engine.ClusterProviderFactory;

public abstract class Settings
{
  private static boolean  firstRun = true;

  private static synchronized void initConstants()
  {
    if (firstRun) {
      firstRun = false;
      ClusterProviderFactory.getProvider();
    }
  }

  // A strategy for type safety, enums and global uniqueness
  public static final Configuration.Setting<String> LOG_DIR_EXPANSION = StringSettings.LOG_DIR_EXPANSION;
  public static final Configuration.Setting<String> CONTAINER_ID = StringSettings.CONTAINER_ID;
  public static final Configuration.Setting<String> JAVA_HOME = StringSettings.JAVA_HOME;
  public static final Configuration.Setting<String> USER = StringSettings.USER;
  public static final Configuration.Setting<String> PWD = StringSettings.PWD;
  public static final Configuration.Setting<String> APPLICATION_CLASSPATH = StringSettings.APPLICATION_CLASSPATH;
  public static final Configuration.Setting<String> DEFAULT_CONTAINER_TEMP_DIR = StringSettings.DEFAULT_CONTAINER_TEMP_DIR;

  public static final Configuration.Setting<String[]> DEFAULT_APPLICATION_CLASSPATH = StringsSettings.DEFAULT_APPLICATION_CLASSPATH;

  private enum StringSettings implements Configuration.Setting<String>
  {
    LOG_DIR_EXPANSION,
    CONTAINER_ID,
    JAVA_HOME,
    USER,
    PWD,
    APPLICATION_CLASSPATH,
    DEFAULT_CONTAINER_TEMP_DIR
  }

  private enum StringsSettings implements Configuration.Setting<String[]>
  {
    DEFAULT_APPLICATION_CLASSPATH
  }

  public enum Strings
  {
    /*
    LOG_DIR_EXPANSION_VAR,
    CONTAINER_ID,
    JAVA_HOME,
    $JAVA_HOME,
    USER,
    $PWD,
    APPLICATION_CLASSPATH,
    DEFAULT_CONTAINER_TEMP_DIR,
    DEFAULT_APPLICATION_CLASSPATH,
    */

    RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
    RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
    RM_WEBAPP_HTTPS_ADDRESS,
    DEFAULT_RM_WEBAPP_HTTPS_ADDRESS,
    RM_WEBAPP_ADDRESS,
    DEFAULT_RM_WEBAPP_ADDRESS,
    RM_PRINCIPAL,
    PROXY_ADDRESS,
    RM_PREFIX,
    RM_ADDRESS,
    DEFAULT_RM_ADDRESS,
    HTTP_POLICY_KEY,
    HTTP_POLICY_DEFAULT,
    NM_LOG_DIRS,
    ACL_ENABLE,
    ADMIN_ACL,
    DEFAULT_ADMIN_ACL;

    static {
      initConstants();
    }

    private String[] values = null;

    public String getValue()
    {
      assert values != null;
      return values[0];
    }

    public String[] getValues()
    {
      assert values != null;
      return values;
    }
  }

  public enum Longs
  {
    DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS;

    static {
      initConstants();
    }

    private long value = -1L;

    public long getValue()
    {
      assert value != -1L;
      return value;
    }
  }

  public enum Integers
  {
    DEFAULT_RM_WEBAPP_HTTPS_PORT,
    DEFAULT_RM_WEBAPP_PORT,
    DEFAULT_RM_PORT;

    static {
      initConstants();
    }

    private int value = -1;

    public int getValue()
    {
      assert value != -1;
      return value;
    }
  }

  public enum Booleans
  {
    DEFAULT_ACL_ENABLE;

    static {
      initConstants();
    }

    private Boolean value = null;

    public boolean getValue()
    {
      assert value != null;
      return value;
    }
  }

  protected void setValue(Strings constant, String value)
  {
    constant.values = new String[] {value};;
  }

  protected void setValue(Strings constant, String[] values)
  {
    constant.values = values;
  }

  protected void setValue(Integers constant, int value)
  {
    constant.value = value;
  }

  protected void setValue(Longs constant, long value)
  {
    constant.value = value;
  }

  protected void setValue(Booleans constant, boolean value)
  {
    constant.value = value;
  }

  protected abstract void init();
}
