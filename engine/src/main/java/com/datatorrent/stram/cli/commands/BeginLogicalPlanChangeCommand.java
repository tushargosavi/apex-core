package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;

import jline.console.ConsoleReader;

public class BeginLogicalPlanChangeCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    apexCli.setChangingLogicalPlan(true);
    reader.setHistory(apexCli.getChangingLogicalPlanHistory());
  }
}
