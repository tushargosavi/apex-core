/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.yarn.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.yarn.security.StramWSFilterInitializer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.datatorrent.api.Context;

public class SecurityUtils extends com.datatorrent.stram.util.SecurityUtils
{
  private static final Logger LOG = LoggerFactory.getLogger(SecurityUtils.class);

  /**
   * Config property name for the SSL keystore location used by Hadoop webapps.
   * This should be replaced when a constant is defined there
   */
  private static final String SSL_SERVER_KEYSTORE_LOCATION = "ssl.server.keystore.location";

  private static final String HADOOP_HTTP_AUTH_SIMPLE_ANONYMOUS_ALLOWED_PROP = "hadoop.http.authentication.simple.anonymous.allowed";

  /**
   * Setup security related configuration for {@link org.apache.hadoop.yarn.webapp.WebApp}.
   * @param config
   * @param sslConfig
   * @return
   */
  public static Configuration configureWebAppSecurity(Configuration config, Context.SSLConfig sslConfig)
  {
    if (isStramWebSecurityEnabled()) {
      config = new Configuration(config);
      config.set("hadoop.http.filter.initializers", StramWSFilterInitializer.class.getCanonicalName());
    } else {
      String authType = config.get(HADOOP_HTTP_AUTH_PROP);
      if (!HADOOP_HTTP_AUTH_VALUE_SIMPLE.equals(authType)) {
        // turn off authentication for Apex as specified by user
        LOG.warn("Found {} {} but authentication was disabled in Apex.", HADOOP_HTTP_AUTH_PROP, authType);
        config = new Configuration(config);
        config.set(HADOOP_HTTP_AUTH_PROP, HADOOP_HTTP_AUTH_VALUE_SIMPLE);
        config.setBoolean(HADOOP_HTTP_AUTH_SIMPLE_ANONYMOUS_ALLOWED_PROP, true);
      }
    }
    if (sslConfig != null) {
      addSSLConfigResource(config, sslConfig);
    }
    return config;
  }

  /**
   * Modify config object by adding SSL related parameters into a resource for WebApp's use
   *
   * @param config  Configuration to be modified
   * @param sslConfig
   */
  private static void addSSLConfigResource(Configuration config, Context.SSLConfig sslConfig)
  {
    String nodeLocalConfig = sslConfig.getConfigPath();
    if (StringUtils.isNotEmpty(nodeLocalConfig)) {
      config.addResource(new Path(nodeLocalConfig));
    } else {
      // create a configuration object and add it as a resource
      Configuration sslConfigResource = new Configuration(false);
      final String SSL_CONFIG_LONG_NAME = Context.DAGContext.SSL_CONFIG.getLongName();
      sslConfigResource.set(SSL_SERVER_KEYSTORE_LOCATION, new Path(sslConfig.getKeyStorePath()).getName(), SSL_CONFIG_LONG_NAME);
      sslConfigResource.set(WebAppUtils.WEB_APP_KEYSTORE_PASSWORD_KEY, sslConfig.getKeyStorePassword(), SSL_CONFIG_LONG_NAME);
      sslConfigResource.set(WebAppUtils.WEB_APP_KEY_PASSWORD_KEY, sslConfig.getKeyStoreKeyPassword(), SSL_CONFIG_LONG_NAME);
      config.addResource(sslConfigResource);
    }
  }
}
