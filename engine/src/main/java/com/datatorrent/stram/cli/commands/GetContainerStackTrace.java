package com.datatorrent.stram.cli.commands;

import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class GetContainerStackTrace implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String containerLongId = apexCli.getContainerLongId(args[1]);
    if (containerLongId == null) {
      throw new CliException("Container " + args[1] + " not found");
    }

    JSONObject response;
    try {
      response = apexCli.getCurrentAppResource(StramWebServices.PATH_PHYSICAL_PLAN_CONTAINERS + "/" + args[1] + "/" + StramWebServices.PATH_STACKTRACE);
    } catch (Exception ex) {
      throw new CliException("Webservice call to AppMaster failed.", ex);
    }

    apexCli.printJson(response);
  }
}
