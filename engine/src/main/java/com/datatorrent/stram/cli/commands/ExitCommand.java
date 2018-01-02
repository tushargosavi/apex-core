package com.datatorrent.stram.cli.commands;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.cli.ApexCli;

import jline.console.ConsoleReader;

public class ExitCommand implements Command
{
  private static final Logger LOG = LoggerFactory.getLogger(ExitCommand.class);

  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    if (apexCli.getTopLevelHistory() != null) {
      try {
        apexCli.getTopLevelHistory().flush();
      } catch (IOException ex) {
        LOG.warn("Cannot flush command history");
      }
    }
    if (apexCli.getChangingLogicalPlanHistory() != null) {
      try {
        apexCli.getChangingLogicalPlanHistory().flush();
      } catch (IOException ex) {
        LOG.warn("Cannot flush command history");
      }
    }
    apexCli.stop();
    System.exit(0);
  }
}
