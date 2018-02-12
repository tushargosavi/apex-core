/**
 * Copyright (c) 2012-2018 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.k8s;

import org.apache.hadoop.util.Shell;

/**
 * Created by sergey on 2/2/18.
 */
public class Settings extends org.apache.apex.engine.api.Settings
{
  @Override
  protected void init()
  {
    // Strings

    /*
    setValue(Strings.LOG_DIR_EXPANSION_VAR, "<LOG_DIR>");
    setValue(Strings.CONTAINER_ID, "CONTAINER_ID");
    setValue(Strings.JAVA_HOME, "JAVA_HOME");
    setValue(Strings.$JAVA_HOME, getEvalVarExp("JAVA_HOME"));
    setValue(Strings.USER, "USER");
    setValue(Strings.$PWD, getEvalVarExp("PWD"));

    setValue(Strings.APPLICATION_CLASSPATH, "yarn.application.classpath");
    setValue(Strings.DEFAULT_CONTAINER_TEMP_DIR, "./tmp");
    setValue(Strings.DEFAULT_APPLICATION_CLASSPATH, new String[] {getEvalVarExp("HADOOP_CONF_DIR")});
    */

    setValue(Strings.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, "yarn.resourcemanager.connect.max-wait.ms");
    setValue(Strings.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, "yarn.resourcemanager.connect.retry-interval.ms");
    setValue(Strings.RM_WEBAPP_HTTPS_ADDRESS, "yarn.resourcemanager.webapp.https.address");
    setValue(Strings.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS, "0.0.0.0:8090");
    setValue(Strings.RM_WEBAPP_ADDRESS, "yarn.resourcemanager.webapp.address");
    setValue(Strings.DEFAULT_RM_WEBAPP_ADDRESS, "0.0.0.0:8088");
    setValue(Strings.RM_PRINCIPAL, "yarn.resourcemanager.principal");
    setValue(Strings.PROXY_ADDRESS, "yarn.web-proxy.address");
    setValue(Strings.RM_PREFIX, "yarn.resourcemanager.");
    setValue(Strings.RM_ADDRESS, "yarn.resourcemanager.address");
    setValue(Strings.DEFAULT_RM_ADDRESS, "0.0.0.0:8032");
    setValue(Strings.HTTP_POLICY_KEY, "yarn.http.policy");
    setValue(Strings.HTTP_POLICY_DEFAULT, "HTTP_ONLY");
    setValue(Strings.NM_LOG_DIRS, "yarn.nodemanager.log-dirs");
    setValue(Strings.ACL_ENABLE, "yarn.acl.enable");
    setValue(Strings.ADMIN_ACL, "yarn.admin.acl");
    setValue(Strings.DEFAULT_ADMIN_ACL, "*");

    // Longs

    setValue(Longs.DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, 900000L);

    // Integers

    setValue(Integers.DEFAULT_RM_WEBAPP_HTTPS_PORT, 8090);
    setValue(Integers.DEFAULT_RM_WEBAPP_PORT, 8088);
    setValue(Integers.DEFAULT_RM_PORT, 8032);

    // Booleans

    setValue(Booleans.DEFAULT_ACL_ENABLE, false);
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
