/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.yarn;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class Settings extends org.apache.apex.engine.api.Settings
{
  @Override
  protected void init()
  {
    // Strings

    /*
    setValue(Strings.LOG_DIR_EXPANSION_VAR, ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    setValue(Strings.CONTAINER_ID, ApplicationConstants.Environment.CONTAINER_ID.name());
    setValue(Strings.JAVA_HOME, ApplicationConstants.Environment.JAVA_HOME.key());
    setValue(Strings.$JAVA_HOME, ApplicationConstants.Environment.JAVA_HOME.$());
    setValue(Strings.USER, ApplicationConstants.Environment.USER.key());
    setValue(Strings.$PWD,ApplicationConstants.Environment.PWD.$());

    setValue(Strings.APPLICATION_CLASSPATH, YarnConfiguration.YARN_APPLICATION_CLASSPATH);
    setValue(Strings.DEFAULT_CONTAINER_TEMP_DIR, YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    setValue(Strings.DEFAULT_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
    */
    setValue(Strings.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS);
    setValue(Strings.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS);
    setValue(Strings.RM_WEBAPP_HTTPS_ADDRESS, YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS);
    setValue(Strings.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS, YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS);
    setValue(Strings.RM_WEBAPP_ADDRESS, YarnConfiguration.RM_WEBAPP_ADDRESS);
    setValue(Strings.DEFAULT_RM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS);
    setValue(Strings.RM_PRINCIPAL, YarnConfiguration.RM_PRINCIPAL);
    setValue(Strings.PROXY_ADDRESS, YarnConfiguration.PROXY_ADDRESS);
    setValue(Strings.RM_PREFIX, YarnConfiguration.RM_PREFIX);
    setValue(Strings.RM_ADDRESS, YarnConfiguration.RM_ADDRESS);
    setValue(Strings.DEFAULT_RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS);
    setValue(Strings.HTTP_POLICY_KEY, YarnConfiguration.YARN_HTTP_POLICY_KEY);
    setValue(Strings.HTTP_POLICY_DEFAULT, YarnConfiguration.YARN_HTTP_POLICY_DEFAULT);
    setValue(Strings.NM_LOG_DIRS, YarnConfiguration.NM_LOG_DIRS);
    setValue(Strings.ACL_ENABLE, YarnConfiguration.YARN_ACL_ENABLE);
    setValue(Strings.ADMIN_ACL, YarnConfiguration.YARN_ADMIN_ACL);
    setValue(Strings.DEFAULT_ADMIN_ACL, YarnConfiguration.DEFAULT_YARN_ADMIN_ACL);

    // Longs

    setValue(Longs.DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS);

    // Integers

    setValue(Integers.DEFAULT_RM_WEBAPP_HTTPS_PORT, YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT);
    setValue(Integers.DEFAULT_RM_WEBAPP_PORT, YarnConfiguration.DEFAULT_RM_WEBAPP_PORT);
    setValue(Integers.DEFAULT_RM_PORT, YarnConfiguration.DEFAULT_RM_PORT);

    // Booleans

    setValue(Booleans.DEFAULT_ACL_ENABLE, YarnConfiguration.DEFAULT_YARN_ACL_ENABLE);
  }
}
