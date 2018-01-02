package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.plan.logical.requests.RemoveStreamRequest;

import jline.console.ConsoleReader;

public class RemoveStreamCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String streamName = args[1];
    RemoveStreamRequest request = new RemoveStreamRequest();
    request.setStreamName(streamName);
    apexCli.getLogicalPlanRequestQueue().add(request);
  }
}
