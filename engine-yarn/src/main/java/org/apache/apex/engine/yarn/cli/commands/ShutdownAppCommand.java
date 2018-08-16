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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONObject;

import org.apache.apex.engine.yarn.cli.ApexCli;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import com.datatorrent.stram.cli.commands.Command;
import com.datatorrent.stram.cli.util.CliException;

import jline.console.ConsoleReader;

public class ShutdownAppCommand implements Command
{

  @Override
  public void execute(String[] args, ConsoleReader reader, com.datatorrent.stram.cli.ApexCli apexClient) throws Exception
  {
    ApexCli apexCli = (ApexCli)apexClient;
    Map<String, ApplicationReport> appIdReports = new LinkedHashMap<>();

    if (args.length == 1) {
      if (!apexCli.isCurrentApp()) {
        throw new CliException("No application selected");
      } else {
        appIdReports.put(apexCli.currentApp.getApplicationId().toString(), apexCli.currentApp);
      }
    } else {
      String[] appNamesOrIds = Arrays.copyOfRange(args, 1, args.length);

      for (String appNameOrId : appNamesOrIds) {
        ApplicationReport ap = apexCli.findApplicationReportFromAppNameOrId(appNameOrId);
        appIdReports.put(appNameOrId, ap);
      }
    }

    for (Map.Entry<String, ApplicationReport> entry : appIdReports.entrySet()) {
      String appNameOrId = entry.getKey();
      ApplicationReport app = entry.getValue();

      shutdownApp(appNameOrId, app, apexCli);
    }
  }

  private void shutdownApp(String appNameOrId, ApplicationReport app, ApexCli apexClient)
  {
    if (app == null) {
      String errMessage = "Failed to request shutdown for app %s: Application with id or name %s not found%n";
      System.err.printf(errMessage, appNameOrId, appNameOrId);
      return;
    }

    try {
      JSONObject response = apexClient.sendShutdownRequest(app);
      if (apexClient.isConsolePresent()) {
        System.out.printf("Shutdown of app %s requested: %s%n", app.getApplicationId().toString(), response);
      }
    } catch (Exception e) {
      String errMessage = "Failed to request shutdown for app %s: %s%n";
      System.err.printf(errMessage, app.getApplicationId().toString(), e.getMessage());
    } finally {
      if (apexClient.isCurrentApp()) {
        if (app.getApplicationId().equals(apexClient.currentApp.getApplicationId())) {
          apexClient.currentApp = null;
        }
      }
    }
  }
}
