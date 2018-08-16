package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.plan.logical.requests.AddStreamSinkRequest;

import jline.console.ConsoleReader;

public class AddStreamSinkCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String streamName = args[1];
    String sinkOperatorName = args[2];
    String sinkPortName = args[3];
    AddStreamSinkRequest request = new AddStreamSinkRequest();
    request.setStreamName(streamName);
    request.setSinkOperatorName(sinkOperatorName);
    request.setSinkOperatorPortName(sinkPortName);
    apexCli.getLogicalPlanRequestQueue().add(request);
  }
}
