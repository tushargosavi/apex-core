/**
 * Copyright (c) 2012-2018 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.k8s.client;

import java.io.File;

import org.apache.apex.engine.ClusterProviderFactory;
import org.apache.apex.engine.k8s.StramClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * Created by sergey on 2/2/18.
 */
public class StramAppLauncher extends com.datatorrent.stram.client.StramAppLauncher
{
  public StramAppLauncher(File appJarFile, Configuration conf) throws Exception
  {
    super(appJarFile, conf);
  }

  public StramAppLauncher(FileSystem fs, Path path, Configuration conf) throws Exception
  {
    super(fs, path, conf);
  }

  public StramAppLauncher(String name, Configuration conf) throws Exception
  {
    super(name, conf);
  }

  public StramAppLauncher(FileSystem fs, Configuration conf) throws Exception
  {
    super(fs, conf);
  }

  /**
   * Submit application to the cluster and return the app id.
   * Sets the context class loader for application dependencies.
   *
   * @param appConfig
   * @return ApplicationId
   * @throws Exception
   */
  public String launchApp(AppFactory appConfig) throws Exception
  {
    loadDependencies();
    Configuration conf = propertiesBuilder.conf;
    conf.setEnum(StreamingApplication.ENVIRONMENT, StreamingApplication.Environment.CLUSTER);
    LogicalPlan dag = appConfig.createApp(propertiesBuilder);
    com.datatorrent.stram.StramClient client = ClusterProviderFactory.getProvider().getStramClient(conf, dag);
    client.startApplication();
    return ((StramClient)client).getAppId();
  }
}
