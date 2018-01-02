package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;

import jline.console.ConsoleReader;

public class AliasCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    if (args[1].equals(args[2])) {
      throw new CliException("Alias to itself!");
    }
    apexCli.getAliases().put(args[1], args[2]);
    if (apexCli.isConsolePresent()) {
      // CHECKSTYLE:OFF:RegexpMultiline
      System.out.println("Alias " + args[1] + " created.");
    }
    apexCli.updateCompleter(reader);
  }
}
