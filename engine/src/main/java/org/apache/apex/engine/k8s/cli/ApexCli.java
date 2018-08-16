/**
 * Copyright (c) 2012-2018 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.k8s.cli;

import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.stram.cli.commands.Command;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.util.WebServicesClient;

/**
 * Created by sergey on 2/2/18.
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
    return new org.apache.apex.engine.k8s.cli.commands.LaunchCommand();
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
