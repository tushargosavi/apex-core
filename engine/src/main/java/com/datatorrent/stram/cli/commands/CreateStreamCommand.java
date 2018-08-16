package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.plan.logical.requests.CreateStreamRequest;

import jline.console.ConsoleReader;

public class CreateStreamCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String streamName = args[1];
    String sourceOperatorName = args[2];
    String sourcePortName = args[3];
    String sinkOperatorName = args[4];
    String sinkPortName = args[5];
    CreateStreamRequest request = new CreateStreamRequest();
    request.setStreamName(streamName);
    request.setSourceOperatorName(sourceOperatorName);
    request.setSinkOperatorName(sinkOperatorName);
    request.setSourceOperatorPortName(sourcePortName);
    request.setSinkOperatorPortName(sinkPortName);
    apexCli.getLogicalPlanRequestQueue().add(request);
  }
}
