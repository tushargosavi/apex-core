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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.yarn.cli.ApexCli;
import org.apache.apex.engine.yarn.client.StramClientUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.datatorrent.stram.cli.commands.Command;
import com.datatorrent.stram.cli.util.CliException;

import jline.console.ConsoleReader;

public class WaitCommand implements Command
{
  private static final Logger LOG = LoggerFactory.getLogger(WaitCommand.class);

  @Override
  public void execute(String[] args, final ConsoleReader reader, com.datatorrent.stram.cli.ApexCli apexClient) throws Exception
  {
    ApexCli apexCli = (ApexCli)apexClient;
    if (!apexCli.isCurrentApp()) {
      throw new CliException("No application selected");
    }
    int timeout = Integer.valueOf(args[1]);

    StramClientUtils.ClientRMHelper.AppStatusCallback cb = new StramClientUtils.ClientRMHelper.AppStatusCallback()
    {
      @Override
      public boolean exitLoop(ApplicationReport report)
      {
        System.out.println("current status is: " + report.getYarnApplicationState());
        try {
          if (reader.getInput().available() > 0) {
            return true;
          }
        } catch (IOException e) {
          LOG.error("Error checking for input.", e);
        }
        return false;
      }
    };

    try {
      StramClientUtils.ClientRMHelper clientRMHelper = new StramClientUtils.ClientRMHelper(apexCli.yarnClient, apexCli.getConf());
      boolean result = clientRMHelper.waitForCompletion(apexCli.currentApp.getApplicationId(), cb, timeout * 1000);
      if (!result) {
        System.err.println("Application terminated unsuccessfully.");
      }
    } catch (YarnException e) {
      throw new CliException("Failed to kill " + apexCli.currentAppIdToString(), e);
    }
  }
}
