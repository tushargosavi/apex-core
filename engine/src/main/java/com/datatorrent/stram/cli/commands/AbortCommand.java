package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;

import jline.console.ConsoleReader;

public class AbortCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    apexCli.getLogicalPlanRequestQueue().clear();
    apexCli.setChangingLogicalPlan(false);
    reader.setHistory(apexCli.getTopLevelHistory());
  }
}
