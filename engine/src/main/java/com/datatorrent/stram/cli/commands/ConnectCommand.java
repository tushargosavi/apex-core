package com.datatorrent.stram.cli.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class ConnectCommand implements Command
{
  private static final Logger LOG = LoggerFactory.getLogger(ConnectCommand.class);

  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    apexCli.setApplication(args[1]);
    if (!apexCli.isCurrentApp()) {
      throw new CliException("Streaming application with id " + args[1] + " is not found.");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Selected {} with tracking url {}", apexCli.currentAppIdToString(), apexCli.getCurrentAppTrackingUrl());
    }
    apexCli.getCurrentAppResource(StramWebServices.PATH_INFO);
    if (apexCli.isConsolePresent()) {
      System.out.println("Connected to application " + apexCli.currentAppIdToString());
    }
  }
}
