package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;

import jline.console.ConsoleReader;

public class SourceCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    apexCli.processSourceFile(args[1], reader);
    if (apexCli.isConsolePresent()) {
      System.out.println("File " + args[1] + " sourced.");
    }
  }
}
