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
package org.apache.apex.engine.local.cli;

import org.codehaus.jettison.json.JSONObject;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.stram.cli.commands.Command;
import com.datatorrent.stram.cli.commands.LaunchCommand;
import com.datatorrent.stram.cli.util.LaunchCommandLineInfo;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.util.WebServicesClient;

/**
 * TODO: Replace trivial implementations of methods as needed
 */
public class ApexCli extends com.datatorrent.stram.cli.ApexCli
{
  @Override
  public JSONObject getCurrentAppResource(String resourcePath)
  {
    return null;
  }

  @Override
  public JSONObject getCurrentAppResource(StramAgent.StramUriSpec uriSpec)
  {
    return null;
  }

  @Override
  public JSONObject getCurrentAppResource(StramAgent.StramUriSpec uriSpec, WebServicesClient.WebServicesHandler handler)
  {
    return null;
  }

  @Override
  public String getCurrentAppId()
  {
    return null;
  }

  @Override
  public void stop()
  {

  }

  @Override
  protected Command getCleanAppDirectoriesCommand()
  {
    return null;
  }

  @Override
  protected Command getGetAppInfoCommand()
  {
    return null;
  }

  @Override
  protected Command getKillAppCommand()
  {
    return null;
  }

  @Override
  protected Command getLaunchCommand()
  {
    return new LaunchCommand()
    {
      @Override
      protected StramAppLauncher.AppFactory checkNonTerminatedApplication(com.datatorrent.stram.cli.ApexCli apexCli, LaunchCommandLineInfo commandLineInfo, StramAppLauncher.AppFactory appFactory, StramAppLauncher submitApp, String matchString)
      {
        return appFactory;
      }

      @Override
      protected void checkDuplicatesAndLaunchApplication(Configuration config, com.datatorrent.stram.cli.ApexCli apexCli, StramAppLauncher.AppFactory appFactory, StramAppLauncher submitApp) throws Exception
      {

      }
    };
  }

  @Override
  protected Command getListAppsCommand()
  {
    return null;
  }

  @Override
  protected Command getShutdownAppCommand()
  {
    return null;
  }

  @Override
  protected Command getWaitCommand()
  {
    return null;
  }

  @Override
  protected void makeApexClientContext()
  {

  }

  @Override
  public boolean isCurrentApp()
  {
    return false;
  }

  @Override
  public String currentAppIdToString()
  {
    return null;
  }

  @Override
  public String getCurrentAppTrackingUrl()
  {
    return null;
  }

  @Override
  public void setApplication(String appId)
  {

  }

  @Override
  public String getContainerLongId(String containerId)
  {
    return null;
  }
}
