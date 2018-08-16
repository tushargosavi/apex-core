package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.plan.logical.requests.CreateOperatorRequest;

import jline.console.ConsoleReader;

public class CreateOperatorCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String operatorName = args[1];
    String className = args[2];
    CreateOperatorRequest request = new CreateOperatorRequest();
    request.setOperatorName(operatorName);
    request.setOperatorFQCN(className);
    apexCli.getLogicalPlanRequestQueue().add(request);
  }
}
