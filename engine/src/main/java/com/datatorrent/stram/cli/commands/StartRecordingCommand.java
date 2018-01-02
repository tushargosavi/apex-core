package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;

import jline.console.ConsoleReader;

public class StartRecordingCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String opId = args[1];
    String port = null;
    long numWindows = 0;
    if (args.length >= 3) {
      port = args[2];
    }
    if (args.length >= 4) {
      numWindows = Long.valueOf(args[3]);
    }
    apexCli.printJson(apexCli.getRecordingsAgent().startRecording(apexCli.getCurrentAppId(), opId, port, numWindows));
  }
}
