/**
 * Copyright (c) 2012-2018 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.k8s.cli.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.LaunchCommandLineInfo;
import com.datatorrent.stram.client.StramAppLauncher;

/**
 * Created by sergey on 2/2/18.
 */
public class LaunchCommand extends com.datatorrent.stram.cli.commands.LaunchCommand
{
  private static final Logger LOG = LoggerFactory.getLogger(LaunchCommand.class);

  @Override
  protected StramAppLauncher.AppFactory checkNonTerminatedApplication(ApexCli apexCli, LaunchCommandLineInfo commandLineInfo, StramAppLauncher.AppFactory appFactory, StramAppLauncher submitApp, String matchString)
  {
    return null;
  }

  @Override
  protected void checkDuplicatesAndLaunchApplication(Configuration config, com.datatorrent.stram.cli.ApexCli apexCli,
      com.datatorrent.stram.client.StramAppLauncher.AppFactory appFactory, com.datatorrent.stram.client.StramAppLauncher submitApp) throws Exception
  {
    String appId = ((org.apache.apex.engine.k8s.client.StramAppLauncher)submitApp).launchApp(appFactory);
    if (appId != null) {
      apexCli.printJson("{\"appId\": \"" + appId + "\"}");
    }

  }
}
