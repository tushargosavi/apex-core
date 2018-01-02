package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.plan.logical.requests.SetOperatorAttributeRequest;

import jline.console.ConsoleReader;

public class SetOperatorAttributeCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String operatorName = args[1];
    String attributeName = args[2];
    String attributeValue = args[3];
    SetOperatorAttributeRequest request = new SetOperatorAttributeRequest();
    request.setOperatorName(operatorName);
    request.setAttributeName(attributeName);
    request.setAttributeValue(attributeValue);
    apexCli.getLogicalPlanRequestQueue().add(request);
  }
}
