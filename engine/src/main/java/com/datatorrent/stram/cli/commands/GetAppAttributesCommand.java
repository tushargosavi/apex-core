package com.datatorrent.stram.cli.commands;

import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class GetAppAttributesCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    if (!apexCli.isCurrentApp()) {
      throw new CliException("No application selected");
    }
    StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
    uriSpec = uriSpec.path(StramWebServices.PATH_LOGICAL_PLAN).path("attributes");
    if (args.length > 1) {
      uriSpec = uriSpec.queryParam("attributeName", args[1]);
    }
    try {
      JSONObject response = apexCli.getCurrentAppResource(uriSpec);
      apexCli.printJson(response);
    } catch (Exception e) {
      throw new CliException("Failed web service request for appid " + apexCli.getCurrentAppId(), e);
    }
  }
}

