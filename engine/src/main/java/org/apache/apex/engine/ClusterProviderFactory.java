/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine;

import org.apache.apex.engine.api.ClusterProvider;
import org.apache.apex.engine.k8s.K8sClusterProvider;
import org.apache.apex.engine.local.LocalClusterProvider;

/**
 *
 */
public class ClusterProviderFactory
{
  public static String PROPERTY_PROVIDER_TYPE = "providerType";
  private static String ENV_PROVIDER_TYPE = "PROVIDER_TYPE";

  private static String LOCAL_PROVIDER_TYPE = "LOCAL";
  private static String YARN_PROVIDER_TYPE = "YARN";
  public static String K8S_PROVIDER_TYPE = "K8S";

  private static String LOCAL_PROVIDER_CLASS_NAME = LocalClusterProvider.class.getCanonicalName();
  private static String YARN_PROVIDER_CLASS_NAME = "org.apache.apex.engine.yarn.YarnClusterProvider";
  private static String K8S_PROVIDER_CLASS_NAME = K8sClusterProvider.class.getCanonicalName();

  private static final ClusterProvider provider = getProviderInstance();

  public static ClusterProvider getProvider()
  {
    return provider;
  }

  public static ClusterProvider getProviderInstance()
  {
    String providerType = System.getProperty(PROPERTY_PROVIDER_TYPE);
    if (providerType == null) {
      providerType = System.getenv(ENV_PROVIDER_TYPE);
    }

    String className = null;
    if ((providerType == null) || providerType.equalsIgnoreCase(YARN_PROVIDER_TYPE)) {
      className = YARN_PROVIDER_CLASS_NAME;
    } else if (providerType.equalsIgnoreCase(K8S_PROVIDER_TYPE)) {
      className = K8S_PROVIDER_CLASS_NAME;
    } else if (providerType.equalsIgnoreCase(LOCAL_PROVIDER_TYPE)) {
      className = LOCAL_PROVIDER_CLASS_NAME;
    } else {
      throw new RuntimeException("Incorrect cluster provider type: " + providerType);
    }

    try {
      Class clazz = Class.forName(className);
      return (ClusterProvider)clazz.newInstance();
    } catch (Exception ex) {
      throw new RuntimeException("No cluster Java class provider found", ex);
    }
  }
}
