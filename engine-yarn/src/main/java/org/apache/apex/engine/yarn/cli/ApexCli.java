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
package org.apache.apex.engine.yarn.cli;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.core.MediaType;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.yarn.cli.commands.CleanAppDirectoriesCommand;
import org.apache.apex.engine.yarn.cli.commands.GetAppInfoCommand;
import org.apache.apex.engine.yarn.cli.commands.KillAppCommand;
import org.apache.apex.engine.yarn.cli.commands.LaunchCommand;
import org.apache.apex.engine.yarn.cli.commands.ListAppsCommand;
import org.apache.apex.engine.yarn.cli.commands.ShutdownAppCommand;
import org.apache.apex.engine.yarn.cli.commands.WaitCommand;
import org.apache.apex.engine.yarn.client.StramClientUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.collect.Sets;
import com.sun.jersey.api.client.WebResource;

import com.datatorrent.stram.StramClient;
import com.datatorrent.stram.cli.commands.Command;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;

public class ApexCli extends com.datatorrent.stram.cli.ApexCli
{
  private static final Logger LOG = LoggerFactory.getLogger(ApexCli.class);

  public YarnClient yarnClient = null;

  public ApplicationReport currentApp = null;

  public YarnClient getYarnClient()
  {
    return yarnClient;
  }

  @Override
  protected void makeApexClientContext()
  {
    yarnClient = StramClientUtils.createYarnClient(conf);
  }

  @Override
  public boolean isCurrentApp()
  {
    return currentApp != null;
  }

  @Override
  public String currentAppIdToString()
  {
    return currentApp.getApplicationId().toString();
  }

  public String getCurrentAppTrackingUrl()
  {
    return currentApp.getTrackingUrl();
  }

  private ApplicationReport assertRunningApp(ApplicationReport app)
  {
    ApplicationReport r;
    try {
      r = yarnClient.getApplicationReport(app.getApplicationId());
      if (r.getYarnApplicationState() != YarnApplicationState.RUNNING) {
        String msg = String.format("Application %s not running (status %s)",
            r.getApplicationId().getId(), r.getYarnApplicationState());
        throw new CliException(msg);
      }
    } catch (YarnException rmExc) {
      throw new CliException("Unable to determine application status", rmExc);
    } catch (IOException rmExc) {
      throw new CliException("Unable to determine application status", rmExc);
    }
    return r;
  }

  @Override
  public JSONObject getCurrentAppResource(String resourcePath)
  {
    return getResource(resourcePath, currentApp);
  }

  public JSONObject getResource(String resourcePath, ApplicationReport appReport)
  {
    return getResource(new StramAgent.StramUriSpec().path(resourcePath), appReport, new WebServicesClient.GetWebServicesHandler<JSONObject>());
  }

  @Override
  public JSONObject getCurrentAppResource(StramAgent.StramUriSpec uriSpec)
  {
    return getResource(uriSpec, currentApp, new WebServicesClient.GetWebServicesHandler<JSONObject>());
  }

  @Override
  public JSONObject getCurrentAppResource(StramAgent.StramUriSpec uriSpec, WebServicesClient.WebServicesHandler handler)
  {
    return getResource(uriSpec, currentApp, handler);
  }

  public JSONObject getResource(StramAgent.StramUriSpec uriSpec, ApplicationReport appReport, WebServicesClient.WebServicesHandler handler)
  {

    if (appReport == null) {
      throw new CliException("No application selected");
    }

    if (StringUtils.isEmpty(appReport.getTrackingUrl()) || appReport.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
      throw new CliException("Application terminated");
    }

    WebServicesClient wsClient = new WebServicesClient();
    try {
      return stramAgent.issueStramWebRequest(wsClient, appReport.getApplicationId().toString(), uriSpec, handler);
    } catch (Exception e) {
      // check the application status as above may have failed due application termination etc.
      if (appReport == currentApp) {
        currentApp = assertRunningApp(appReport);
      }
      throw new CliException("Failed to request web service for appid " + appReport.getApplicationId().toString(), e);
    }
  }

  public String getContainerLongId(String containerId)
  {
    JSONObject json = getResource(StramWebServices.PATH_PHYSICAL_PLAN_CONTAINERS, currentApp);
    int shortId = 0;
    if (StringUtils.isNumeric(containerId)) {
      shortId = Integer.parseInt(containerId);
    }
    try {
      Object containersObj = json.get("containers");
      JSONArray containers;
      if (containersObj instanceof JSONArray) {
        containers = (JSONArray)containersObj;
      } else {
        containers = new JSONArray();
        containers.put(containersObj);
      }
      if (containersObj != null) {
        for (int o = containers.length(); o-- > 0; ) {
          JSONObject container = containers.getJSONObject(o);
          String id = container.getString("id");
          if (id.equals(containerId) || (shortId != 0 && (id.endsWith("_" + shortId) || id.endsWith("0" + shortId)))) {
            return id;
          }
        }
      }
    } catch (JSONException ex) {
      // ignore
    }
    return null;
  }

  public List<ApplicationReport> getApplicationList()
  {
    try {
      return yarnClient.getApplications(Sets.newHashSet(StramClient.APPLICATION_TYPE, StramClient.APPLICATION_TYPE_DEPRECATED));
    } catch (Exception e) {
      throw new CliException("Error getting application list from resource manager", e);
    }
  }

  public ApplicationReport getApplication(String appId)
  {
    List<ApplicationReport> appList = getApplicationList();
    if (StringUtils.isNumeric(appId)) {
      int appSeq = Integer.parseInt(appId);
      for (ApplicationReport ar : appList) {
        if (ar.getApplicationId().getId() == appSeq) {
          return ar;
        }
      }
    } else {
      for (ApplicationReport ar : appList) {
        if (ar.getApplicationId().toString().equals(appId)) {
          return ar;
        }
      }
    }
    return null;
  }

  @Override
  public void setApplication(String appId)
  {
    currentApp = getApplication(appId);
  }

  public ApplicationReport getApplicationByName(String appName)
  {
    if (appName == null) {
      throw new CliException("Invalid application name provided by user");
    }
    List<ApplicationReport> appList = getApplicationList();
    for (ApplicationReport ar : appList) {
      if ((ar.getName().equals(appName)) &&
          (ar.getYarnApplicationState() != YarnApplicationState.KILLED) &&
          (ar.getYarnApplicationState() != YarnApplicationState.FINISHED)) {
        LOG.debug("Application Name: {} Application ID: {} Application State: {}",
            ar.getName(), ar.getApplicationId().toString(), YarnApplicationState.FINISHED);
        return ar;
      }
    }
    return null;
  }

  public ApplicationReport findApplicationReportFromAppNameOrId(String appNameOrId)
  {
    ApplicationReport app = getApplication(appNameOrId);
    if (app == null) {
      app = getApplicationByName(appNameOrId);
    }
    return app;
  }

  @Override
  public String getCurrentAppId()
  {
    return currentApp.getApplicationId().toString();
  }


  public JSONObject sendShutdownRequest(ApplicationReport app)
  {
    StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec().path(StramWebServices.PATH_SHUTDOWN);

    WebServicesClient.WebServicesHandler<JSONObject> handler = new WebServicesClient.WebServicesHandler<JSONObject>()
    {
      @Override
      public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
      {
        return webResource.accept(MediaType.APPLICATION_JSON).post(clazz, new JSONObject());
      }
    };

    return getResource(uriSpec, app, handler);
  }

  @Override
  public void stop()
  {
    yarnClient.stop();
  }

  @Override
  protected Command getCleanAppDirectoriesCommand()
  {
    return new CleanAppDirectoriesCommand();
  }

  @Override
  protected Command getGetAppInfoCommand()
  {
    return new GetAppInfoCommand();
  }

  @Override
  protected Command getKillAppCommand()
  {
    return new KillAppCommand();
  }

  @Override
  protected Command getLaunchCommand()
  {
    return new LaunchCommand();
  }

  @Override
  protected Command getListAppsCommand()
  {
    return new ListAppsCommand();
  }

  @Override
  protected Command getShutdownAppCommand()
  {
    return new ShutdownAppCommand();
  }

  @Override
  protected Command getWaitCommand()
  {
    return new WaitCommand();
  }
}
