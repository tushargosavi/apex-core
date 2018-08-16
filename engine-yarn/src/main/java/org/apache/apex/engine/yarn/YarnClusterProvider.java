/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.yarn;

import java.io.File;

import org.apache.apex.engine.api.Clock;
import org.apache.apex.engine.api.ClusterProvider;
import org.apache.apex.engine.api.StreamingAppMaster;
import org.apache.apex.engine.api.security.TokenManager;
import org.apache.apex.engine.yarn.cli.ApexCli;
import org.apache.apex.engine.yarn.client.StramAgent;
import org.apache.apex.engine.yarn.client.StramAppLauncher;
import org.apache.apex.engine.yarn.client.StramClientUtils;
import org.apache.apex.engine.yarn.security.delegation.DelegationTokenManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.datatorrent.api.Context;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.security.StramDelegationTokenManager;

/**
 * Created by pi on 11/7/17.
 */
public class YarnClusterProvider implements ClusterProvider
{
  private static final long DELEGATION_KEY_UPDATE_INTERVAL = 24 * 60 * 60 * 1000;
  private static final long DELEGATION_TOKEN_MAX_LIFETIME = 90 * 60 * 1000;
  private static final long DELEGATION_TOKEN_RENEW_INTERVAL = 90 * 60 * 1000;
  private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL = 30 * 60 * 1000;

  private org.apache.apex.engine.api.Configuration configuration;

  public YarnClusterProvider()
  {
    new Settings().init();
  }

  @Override
  public StreamingAppMaster getStreamingAppMaster()
  {
    return new StreamingAppMasterService();
  }

  @Override
  public StreamingContainerManager getStreamingContainerManager(LogicalPlan dag)
  {
    return new org.apache.apex.engine.yarn.StreamingContainerManager(dag);
  }

  @Override
  public StreamingContainerManager getStreamingContainerManager(LogicalPlan dag, Clock clock)
  {
    return new org.apache.apex.engine.yarn.StreamingContainerManager(dag, clock);
  }

  @Override
  public StreamingContainerManager getStreamingContainerManager(LogicalPlan dag, boolean enableEventRecording, Clock clock)
  {
    return new org.apache.apex.engine.yarn.StreamingContainerManager(dag, enableEventRecording, clock);
  }

  @Override
  public StreamingContainerManager getStreamingContainerManager(StreamingContainerManager.CheckpointState checkpointedState, boolean enableEventRecording)
  {
    return new org.apache.apex.engine.yarn.StreamingContainerManager(checkpointedState, enableEventRecording);
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
  public com.datatorrent.stram.client.StramAgent getStramAgent(FileSystem fs, Configuration conf)
  {
    return new StramAgent(fs, conf);
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
    return ConverterUtils.toContainerId(value).toString();
  }

  @Override
  public String getApplicationId(String value)
  {
    return ConverterUtils.toContainerId(value).getApplicationAttemptId().getApplicationId().toString();
  }

  @Override
  public synchronized org.apache.apex.engine.api.Configuration getConfiguration()
  {
    if (configuration == null) {
      configuration = new YarnEngineConfiguration(new YarnConfiguration());
    }
    return configuration;
  }

  @Override
  public void addRMDelegationToken(Configuration conf, String tokenRenewer, Credentials creds) throws Exception
  {
    try (YarnClient yarnClient = StramClientUtils.createYarnClient(conf)) {
      new StramClientUtils.ClientRMHelper(yarnClient, conf).addRMDelegationToken(tokenRenewer, creds);
    }
  }

  @Override
  public TokenManager<? extends Context> getTokenManager()
  {
    return new DelegationTokenManager(new StramDelegationTokenManager(DELEGATION_KEY_UPDATE_INTERVAL, DELEGATION_TOKEN_MAX_LIFETIME, DELEGATION_TOKEN_RENEW_INTERVAL, DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL));
  }
}
