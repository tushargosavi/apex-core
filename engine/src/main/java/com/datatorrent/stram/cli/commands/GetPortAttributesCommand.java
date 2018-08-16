package com.datatorrent.stram.cli.commands;

import java.net.URLEncoder;

import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class GetPortAttributesCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    if (!apexCli.isCurrentApp()) {
      throw new CliException("No application selected");
    }
    StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
    uriSpec = uriSpec.path(StramWebServices.PATH_LOGICAL_PLAN_OPERATORS).path(URLEncoder.encode(args[1], "UTF-8")).path("ports").path(URLEncoder.encode(args[2], "UTF-8")).path("attributes");
    if (args.length > 3) {
      uriSpec = uriSpec.queryParam("attributeName", args[3]);
    }
    try {
      JSONObject response = apexCli.getCurrentAppResource(uriSpec);
      apexCli.printJson(response);
    } catch (Exception e) {
      throw new CliException("Failed web service request for appid " + apexCli.getCurrentAppId(), e);
    }
  }
}
