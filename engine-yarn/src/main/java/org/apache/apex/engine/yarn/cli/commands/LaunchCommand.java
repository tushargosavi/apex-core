/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.yarn.cli.commands;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.yarn.cli.ApexCli;
import org.apache.apex.engine.yarn.client.StramAppLauncher;
import org.apache.apex.engine.yarn.client.StramClientUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.cli.util.LaunchCommandLineInfo;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

public class LaunchCommand extends com.datatorrent.stram.cli.commands.LaunchCommand
{
  private static final Logger LOG = LoggerFactory.getLogger(LaunchCommand.class);

  @Override
  protected com.datatorrent.stram.client.StramAppLauncher.AppFactory checkNonTerminatedApplication(com.datatorrent.stram.cli.ApexCli apexCli,
      LaunchCommandLineInfo commandLineInfo, com.datatorrent.stram.client.StramAppLauncher.AppFactory appFactory,
      com.datatorrent.stram.client.StramAppLauncher submitApp, String matchString)
  {
    // ensure app is not running
    ApplicationReport ar = null;
    try {
      ar = ((ApexCli)apexCli).getApplication(commandLineInfo.origAppId);
    } catch (Exception e) {
      // application (no longer) in the RM history, does not prevent restart from state in DFS
      LOG.debug("Cannot determine status of application {} {}", commandLineInfo.origAppId, ExceptionUtils.getMessage(e));
    }
    if (ar != null) {
      if (ar.getFinalApplicationStatus() == FinalApplicationStatus.UNDEFINED) {
        throw new CliException("Cannot relaunch non-terminated application: " + commandLineInfo.origAppId + " " + ar.getYarnApplicationState());
      }
      if (appFactory == null && matchString == null) {
        // skip selection if we can match application name from previous run
        List<com.datatorrent.stram.client.StramAppLauncher.AppFactory> matchingAppFactories = apexCli.getMatchingAppFactories(submitApp, ar.getName(), commandLineInfo.exactMatch);
        for (com.datatorrent.stram.client.StramAppLauncher.AppFactory af : matchingAppFactories) {
          String appName = submitApp.getLogicalPlanConfiguration().getAppAlias(af.getName());
          if (appName == null) {
            appName = af.getName();
          }
          // limit to exact match
          if (appName.equals(ar.getName())) {
            appFactory = af;
            break;
          }
        }
      }
    }
    return appFactory;
  }

  @Override
  protected void checkDuplicatesAndLaunchApplication(Configuration config, com.datatorrent.stram.cli.ApexCli apexCli,
      com.datatorrent.stram.client.StramAppLauncher.AppFactory appFactory, com.datatorrent.stram.client.StramAppLauncher submitApp) throws Exception
  {
    // see whether there is an app with the same name and user name running
    String appName = config.get(LogicalPlanConfiguration.KEY_APPLICATION_NAME, appFactory.getName());
    ApplicationReport duplicateApp = StramClientUtils.getStartedAppInstanceByName(((ApexCli)apexCli).yarnClient, appName, UserGroupInformation.getLoginUser().getUserName(), null);
    if (duplicateApp != null) {
      throw new CliException("Application with the name \"" + duplicateApp.getName() + "\" already running under the current user \"" + duplicateApp.getUser() + "\". Please choose another name. You can change the name by setting " + LogicalPlanConfiguration.KEY_APPLICATION_NAME);
    }

    // This is for suppressing System.out printouts from applications so that the user of CLI will not be confused by those printouts
    PrintStream originalStream = apexCli.suppressOutput();
    ApplicationId appId = null;
    try {
      if (apexCli.isRaw()) {
        PrintStream dummyStream = new PrintStream(new OutputStream()
        {
          @Override
          public void write(int b)
          {
            // no-op
          }

        });
        System.setOut(dummyStream);
      }
      appId = ((StramAppLauncher)submitApp).launchApp(appFactory);
      ((ApexCli)apexCli).currentApp = ((ApexCli)apexCli).yarnClient.getApplicationReport(appId);
    } finally {
      apexCli.restoreOutput(originalStream);
    }
    if (appId != null) {
      apexCli.printJson("{\"appId\": \"" + appId + "\"}");
    }
  }
}
