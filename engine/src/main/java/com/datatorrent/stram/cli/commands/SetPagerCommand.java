package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;

import jline.console.ConsoleReader;

public class SetPagerCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    if (args[1].equals("off")) {
      apexCli.setPagerCommand(null);
    } else if (args[1].equals("on")) {
      if (apexCli.isConsolePresent()) {
        apexCli.setPagerCommand("less -F -X -r");
      }
    } else {
      throw new CliException("set-pager parameter is either on or off.");
    }
  }
}
