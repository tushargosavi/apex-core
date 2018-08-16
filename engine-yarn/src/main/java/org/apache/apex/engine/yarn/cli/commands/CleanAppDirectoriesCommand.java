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

import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import org.apache.apex.engine.yarn.cli.ApexCli;
import org.apache.apex.engine.yarn.client.StramClientUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import com.datatorrent.stram.cli.commands.Command;

import jline.console.ConsoleReader;

public class CleanAppDirectoriesCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, com.datatorrent.stram.cli.ApexCli apexClient) throws Exception
  {
    ApexCli apexCli = (ApexCli)apexClient;
    JSONObject result = new JSONObject();
    JSONArray appArray = new JSONArray();
    List<ApplicationReport> apps = StramClientUtils.cleanAppDirectories(apexCli.yarnClient, apexCli.getConf(), apexCli.getFs(),
        System.currentTimeMillis() - Long.valueOf(args[1]));
    for (ApplicationReport app : apps) {
      appArray.put(app.getApplicationId().toString());
    }
    result.put("applications", appArray);
    apexCli.printJson(result);
  }
}
