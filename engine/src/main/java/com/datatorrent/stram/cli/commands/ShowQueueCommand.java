package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;

import jline.console.ConsoleReader;

public class ShowQueueCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    apexCli.printJson(apexCli.getLogicalPlanRequestQueue(), "queue");
    if (apexCli.isConsolePresent()) {
      System.out.println("Total operations in queue: " + apexCli.getLogicalPlanRequestQueue().size());
    }
  }
}
