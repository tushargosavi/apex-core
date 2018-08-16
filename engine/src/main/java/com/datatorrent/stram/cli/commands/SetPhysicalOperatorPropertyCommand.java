package com.datatorrent.stram.cli.commands;

import javax.ws.rs.core.MediaType;

import org.codehaus.jettison.json.JSONObject;

import org.apache.commons.lang.math.NumberUtils;

import com.sun.jersey.api.client.WebResource;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class SetPhysicalOperatorPropertyCommand implements Command
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
    StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
    uriSpec = uriSpec.path(StramWebServices.PATH_PHYSICAL_PLAN_OPERATORS).path(args[1]).path("properties");
    final JSONObject request = new JSONObject();
    request.put(args[2], args[3]);
    JSONObject response = apexCli.getCurrentAppResource(uriSpec, new WebServicesClient.WebServicesHandler<JSONObject>()
    {
      @Override
      public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
      {
        return webResource.accept(MediaType.APPLICATION_JSON).post(JSONObject.class, request);
      }

    });
    apexCli.printJson(response);
  }
}
