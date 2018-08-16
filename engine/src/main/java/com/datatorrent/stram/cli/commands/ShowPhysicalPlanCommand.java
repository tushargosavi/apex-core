package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class ShowPhysicalPlanCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    try {
      apexCli.printJson(apexCli.getCurrentAppResource(StramWebServices.PATH_PHYSICAL_PLAN));
    } catch (Exception e) {
      throw new CliException("Failed web service request for appid " + apexCli.getCurrentAppId(), e);
    }
  }
}
