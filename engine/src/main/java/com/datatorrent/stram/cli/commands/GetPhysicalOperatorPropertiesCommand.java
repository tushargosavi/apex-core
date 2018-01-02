package com.datatorrent.stram.cli.commands;

import org.codehaus.jettison.json.JSONObject;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.math.NumberUtils;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.cli.util.GetPhysicalPropertiesCommandLineOptions;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class GetPhysicalOperatorPropertiesCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    if (!apexCli.isCurrentApp()) {
      throw new CliException("No application selected");
    }
    if (!NumberUtils.isDigits(args[1])) {
      throw new CliException("Operator ID must be a number");
    }
    String[] newArgs = new String[args.length - 1];
    System.arraycopy(args, 1, newArgs, 0, args.length - 1);
    PosixParser parser = new PosixParser();
    CommandLine line = parser.parse(GetPhysicalPropertiesCommandLineOptions.GET_PHYSICAL_PROPERTY_OPTIONS.options, newArgs);
    String waitTime = line.getOptionValue(GetPhysicalPropertiesCommandLineOptions.GET_PHYSICAL_PROPERTY_OPTIONS.waitTime.getOpt());
    String propertyName = line.getOptionValue(GetPhysicalPropertiesCommandLineOptions.GET_PHYSICAL_PROPERTY_OPTIONS.propertyName.getOpt());
    StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
    uriSpec = uriSpec.path(StramWebServices.PATH_PHYSICAL_PLAN_OPERATORS).path(args[1]).path("properties");
    if (propertyName != null) {
      uriSpec = uriSpec.queryParam("propertyName", propertyName);
    }
    if (waitTime != null) {
      uriSpec = uriSpec.queryParam("waitTime", waitTime);
    }

    try {
      JSONObject response = apexCli.getCurrentAppResource(uriSpec);
      apexCli.printJson(response);
    } catch (Exception e) {
      throw new CliException("Failed web service request for appid " + apexCli.getCurrentAppId(), e);
    }
  }
}
