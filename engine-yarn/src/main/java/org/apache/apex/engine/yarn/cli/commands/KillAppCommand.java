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

import org.apache.apex.engine.yarn.cli.ApexCli;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.datatorrent.stram.cli.commands.Command;
import com.datatorrent.stram.cli.util.CliException;

import jline.console.ConsoleReader;

public class KillAppCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, com.datatorrent.stram.cli.ApexCli apexClient) throws Exception
  {
    ApexCli apexCli = (ApexCli)apexClient;
    if (args.length == 1) {
      if (!apexCli.isCurrentApp()) {
        throw new CliException("No application selected");
      } else {
        try {
          apexCli.yarnClient.killApplication(apexCli.currentApp.getApplicationId());
          apexCli.currentApp = null;
        } catch (YarnException e) {
          throw new CliException("Failed to kill " + apexCli.currentApp.getApplicationId(), e);
        }
      }
      if (apexCli.isConsolePresent()) {
        System.out.println("Kill app requested");
      }
      return;
    }

    ApplicationReport app = null;
    int i = 0;
    try {
      while (++i < args.length) {
        app = apexCli.findApplicationReportFromAppNameOrId(args[i]);
        if (app == null) {
          throw new CliException("Streaming application with id or name " + args[i] + " is not found.");
        }

        apexCli.yarnClient.killApplication(app.getApplicationId());
        if (app == apexCli.currentApp) {
          apexCli.currentApp = null;
        }
      }
      if (apexCli.isConsolePresent()) {
        System.out.println("Kill app requested");
      }
    } catch (YarnException e) {
      throw new CliException("Failed to kill " + ((app == null || app.getApplicationId() == null) ? "unknown application" : app.getApplicationId()) + ". Aborting killing of any additional applications.", e);
    } catch (NumberFormatException nfe) {
      throw new CliException("Invalid application Id " + args[i], nfe);
    }
  }
}

