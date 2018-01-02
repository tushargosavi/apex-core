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

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import org.apache.apex.engine.yarn.cli.ApexCli;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import com.datatorrent.stram.cli.commands.Command;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class GetAppInfoCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, com.datatorrent.stram.cli.ApexCli apexClient) throws Exception
  {
    ApexCli apexCli = (ApexCli)apexClient;
    ApplicationReport appReport;
    if (args.length > 1) {
      appReport = apexCli.getApplication(args[1]);
      if (appReport == null) {
        throw new CliException("Streaming application with id " + args[1] + " is not found.");
      }
    } else {
      if (!apexCli.isCurrentApp()) {
        throw new CliException("No application selected");
      }
      // refresh the state in currentApp
      apexCli.currentApp = apexCli.yarnClient.getApplicationReport(apexCli.currentApp.getApplicationId());
      appReport = apexCli.currentApp;
    }
    JSONObject response;
    try {
      response = apexCli.getCurrentAppResource(StramWebServices.PATH_INFO);
    } catch (Exception ex) {
      response = new JSONObject();
      response.put("startTime", appReport.getStartTime());
      response.put("id", appReport.getApplicationId().toString());
      response.put("name", appReport.getName());
      response.put("user", appReport.getUser());
    }
    response.put("state", appReport.getYarnApplicationState().name());
    response.put("trackingUrl", appReport.getTrackingUrl());
    response.put("finalStatus", appReport.getFinalApplicationStatus());
    JSONArray tags = new JSONArray();
    for (String tag : appReport.getApplicationTags()) {
      tags.put(tag);
    }
    response.put("tags", tags);
    apexCli.printJson(response);
  }
}
