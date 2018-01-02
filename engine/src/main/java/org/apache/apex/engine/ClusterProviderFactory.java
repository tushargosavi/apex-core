/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine;

import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.apex.engine.api.ClusterProvider;
import org.apache.apex.engine.local.LocalClusterProvider;

/**
 *
 */
public class ClusterProviderFactory
{
  private static ClusterProvider provider;

  public static ClusterProvider getProvider()
  {
    return getProvider(true);
  }

  public static ClusterProvider getProvider(boolean local)
  {
    if (provider == null) {
      synchronized (ClusterProvider.class) {
        if (provider == null) {
          ServiceLoader<ClusterProvider> loader = ServiceLoader.load(ClusterProvider.class);
          Iterator<ClusterProvider> iterator = loader.iterator();
          if (!iterator.hasNext()) {
            if (local) {
              provider = new LocalClusterProvider();
            }
          } else {
            provider = iterator.next();
          }
          if (provider == null) {
            throw new RuntimeException("No cluster provider found");
          }
        }
      }
    }
    return provider;
  }
}
