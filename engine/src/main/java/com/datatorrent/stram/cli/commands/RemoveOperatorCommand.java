package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.plan.logical.requests.RemoveOperatorRequest;

import jline.console.ConsoleReader;

public class RemoveOperatorCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String operatorName = args[1];
    RemoveOperatorRequest request = new RemoveOperatorRequest();
    request.setOperatorName(operatorName);
    apexCli.getLogicalPlanRequestQueue().add(request);
  }
}
