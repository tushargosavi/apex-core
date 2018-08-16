/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.yarn;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.datatorrent.stram.util.IApplicationAttemptId;

public class ApplicationAttemptId implements IApplicationAttemptId
{
  private org.apache.hadoop.yarn.api.records.ApplicationAttemptId appAttemptID;

  public ApplicationAttemptId(CommandLine cliParser)
  {
    makeApplicationAttemptId(cliParser);
  }

  private void makeApplicationAttemptId(CommandLine cliParser)
  {
    Map<String, String> envs = System.getenv();
    appAttemptID = Records.newRecord(org.apache.hadoop.yarn.api.records.ApplicationAttemptId.class);
    if (!envs.containsKey(ApplicationConstants.Environment.CONTAINER_ID.name())) {
      if (cliParser.hasOption("app_attempt_id")) {
        String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
        appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
      } else {
        throw new IllegalArgumentException("Application Attempt Id not set in the environment");
      }
    } else {
      ContainerId containerId = ConverterUtils.toContainerId(envs.get(ApplicationConstants.Environment.CONTAINER_ID.name()));
      appAttemptID = containerId.getApplicationAttemptId();
    }
  }

  public int getApplicationId()
  {
    return appAttemptID.getApplicationId().getId();
  }

  @Override
  public String getApplicationID()
  {
    return appAttemptID.getApplicationId().toString();
  }

  @Override
  public int getApplicationAttemptId()
  {
    return appAttemptID.getAttemptId();
  }

  @Override
  public long getClusterTimestamp()
  {
    return appAttemptID.getApplicationId().getClusterTimestamp();
  }
}
