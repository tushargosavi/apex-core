package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.plan.logical.requests.SetStreamAttributeRequest;

import jline.console.ConsoleReader;

public class SetStreamAttributeCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String streamName = args[1];
    String attributeName = args[2];
    String attributeValue = args[3];
    SetStreamAttributeRequest request = new SetStreamAttributeRequest();
    request.setStreamName(streamName);
    request.setAttributeName(attributeName);
    request.setAttributeValue(attributeValue);
    apexCli.getLogicalPlanRequestQueue().add(request);
  }
}
