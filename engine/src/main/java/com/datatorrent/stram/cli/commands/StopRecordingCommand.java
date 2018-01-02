package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;

import jline.console.ConsoleReader;

public class StopRecordingCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String opId = args[1];
    String port = null;
    if (args.length == 3) {
      port = args[2];
    }
    apexCli.printJson(apexCli.getRecordingsAgent().stopRecording(apexCli.getCurrentAppId(), opId, port));
  }
}
