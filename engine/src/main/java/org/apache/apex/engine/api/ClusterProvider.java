/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.api;

import java.io.File;

import org.apache.apex.engine.api.security.TokenManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;

import com.datatorrent.api.Context;
import com.datatorrent.stram.StramClient;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * The methods in the interface are subject to change till there an alternate implementation to YARN is ready
 */
public interface ClusterProvider
{

  StreamingAppMaster getStreamingAppMaster();

  StreamingContainerManager getStreamingContainerManager(LogicalPlan dag);

  StreamingContainerManager getStreamingContainerManager(LogicalPlan dag, Clock clock);

  StreamingContainerManager getStreamingContainerManager(LogicalPlan dag, boolean enableEventRecording, Clock clock);

  StreamingContainerManager getStreamingContainerManager(StreamingContainerManager.CheckpointState checkpointedState, boolean enableEventRecording);

  StramAppLauncher getStramAppLauncher(File appJarFile, Configuration conf) throws Exception;

  StramAppLauncher getStramAppLauncher(FileSystem fs, Path path, Configuration conf) throws Exception;

  StramAppLauncher getStramAppLauncher(String name, Configuration conf) throws Exception;

  StramAppLauncher getStramAppLauncher(FileSystem fs, Configuration conf) throws Exception;

  StramAgent getStramAgent(FileSystem fs, Configuration conf);

  StramClient getStramClient(Configuration conf, LogicalPlan dag) throws Exception;

  ApexCli getApexCli();

  String getContainerId(String value);

  String getApplicationId(String value);

  org.apache.apex.engine.api.Configuration getConfiguration();

  void addRMDelegationToken(Configuration conf, String tokenRenewer, Credentials creds) throws Exception;

  TokenManager<? extends Context> getTokenManager();
}
