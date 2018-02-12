/**
 * Copyright (c) 2012-2018 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.k8s;

import java.io.File;

import org.apache.apex.engine.api.Clock;
import org.apache.apex.engine.api.ClusterProvider;
import org.apache.apex.engine.api.StreamingAppMaster;
import org.apache.apex.engine.api.security.TokenManager;
import org.apache.apex.engine.k8s.cli.ApexCli;
import org.apache.apex.engine.k8s.client.StramAppLauncher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;

import com.datatorrent.api.Context;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * Created by sergey on 2/2/18.
 */
public class K8sClusterProvider implements ClusterProvider
{
  public K8sClusterProvider()
  {
    new Settings().init();
  }

  @Override
  public StreamingAppMaster getStreamingAppMaster()
  {
    return null;
  }

  @Override
  public com.datatorrent.stram.StreamingContainerManager getStreamingContainerManager(LogicalPlan dag)
  {
    return new StreamingContainerManager(dag);
  }

  @Override
  public com.datatorrent.stram.StreamingContainerManager getStreamingContainerManager(LogicalPlan dag, Clock clock)
  {
    return new StreamingContainerManager(dag, clock);
  }

  @Override
  public com.datatorrent.stram.StreamingContainerManager getStreamingContainerManager(LogicalPlan dag, boolean enableEventRecording, Clock clock)
  {
    return new StreamingContainerManager(dag, enableEventRecording, clock);
  }

  @Override
  public com.datatorrent.stram.StreamingContainerManager getStreamingContainerManager(com.datatorrent.stram.StreamingContainerManager.CheckpointState checkpointedState, boolean enableEventRecording)
  {
    return new StreamingContainerManager(checkpointedState, enableEventRecording);
  }

  @Override
  public com.datatorrent.stram.client.StramAppLauncher getStramAppLauncher(File appJarFile, Configuration conf) throws Exception
  {
    return new StramAppLauncher(appJarFile, conf);
  }

  @Override
  public com.datatorrent.stram.client.StramAppLauncher getStramAppLauncher(FileSystem fs, Path path, Configuration conf) throws Exception
  {
    return new StramAppLauncher(fs, path, conf);
  }

  @Override
  public com.datatorrent.stram.client.StramAppLauncher getStramAppLauncher(String name, Configuration conf) throws Exception
  {
    return new StramAppLauncher(name, conf);
  }

  @Override
  public com.datatorrent.stram.client.StramAppLauncher getStramAppLauncher(FileSystem fs, Configuration conf) throws Exception
  {
    return new StramAppLauncher(fs, conf);
  }

  @Override
  public StramAgent getStramAgent(FileSystem fs, Configuration conf)
  {
    return null;
  }

  @Override
  public com.datatorrent.stram.StramClient getStramClient(Configuration conf, LogicalPlan dag) throws Exception
  {
    return new StramClient(conf, dag);
  }

  @Override
  public com.datatorrent.stram.cli.ApexCli getApexCli()
  {
    return new ApexCli();
  }

  @Override
  public String getContainerId(String value)
  {
    return "ContainerId";
  }

  @Override
  public String getApplicationId(String value)
  {
    return "ApplicationId";
  }

  @Override
  public org.apache.apex.engine.api.Configuration getConfiguration()
  {
    return new K8slConfiguration();
  }

  @Override
  public void addRMDelegationToken(Configuration conf, String tokenRenewer, Credentials creds) throws Exception
  {

  }

  @Override
  public TokenManager<? extends Context> getTokenManager()
  {
    return null;
  }
}
