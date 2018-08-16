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

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import org.apache.apex.engine.yarn.cli.ApexCli;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import com.datatorrent.stram.cli.commands.Command;
import com.datatorrent.stram.cli.util.CliException;

import jline.console.ConsoleReader;

public class ListAppsCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, com.datatorrent.stram.cli.ApexCli apexClient) throws Exception
  {
    ApexCli apexCli = (ApexCli)apexClient;
    try {
      JSONArray jsonArray = new JSONArray();
      List<ApplicationReport> appList = apexCli.getApplicationList();
      Collections.sort(appList, new Comparator<ApplicationReport>()
      {
        @Override
        public int compare(ApplicationReport o1, ApplicationReport o2)
        {
          return o1.getApplicationId().getId() - o2.getApplicationId().getId();
        }

      });
      int totalCnt = 0;
      int runningCnt = 0;

      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");

      for (ApplicationReport ar : appList) {
          /*
           * This is inefficient, but what the heck, if this can be passed through the command line, can anyone notice slowness.
           */
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("startTime", sdf.format(new java.util.Date(ar.getStartTime())));
        jsonObj.put("id", ar.getApplicationId().getId());
        jsonObj.put("name", ar.getName());
        jsonObj.put("state", ar.getYarnApplicationState().name());
        jsonObj.put("trackingUrl", ar.getTrackingUrl());
        jsonObj.put("finalStatus", ar.getFinalApplicationStatus());
        JSONArray tags = new JSONArray();
        for (String tag : ar.getApplicationTags()) {
          tags.put(tag);
        }
        jsonObj.put("tags", tags);

        totalCnt++;
        if (ar.getYarnApplicationState() == YarnApplicationState.RUNNING) {
          runningCnt++;
        }

        if (args.length > 1) {
          if (StringUtils.isNumeric(args[1])) {
            if (jsonObj.getString("id").equals(args[1])) {
              jsonArray.put(jsonObj);
              break;
            }
          } else {
            @SuppressWarnings("unchecked")
            Iterator<String> keys = jsonObj.keys();
            while (keys.hasNext()) {
              if (jsonObj.get(keys.next()).toString().toLowerCase().contains(args[1].toLowerCase())) {
                jsonArray.put(jsonObj);
                break;
              }
            }
          }
        } else {
          jsonArray.put(jsonObj);
        }
      }
      apexCli.printJson(jsonArray, "apps");
      if (apexCli.isConsolePresent()) {
        System.out.println(runningCnt + " active, total " + totalCnt + " applications.");
      }
    } catch (Exception ex) {
      throw new CliException("Failed to retrieve application list", ex);
    }
  }
}
