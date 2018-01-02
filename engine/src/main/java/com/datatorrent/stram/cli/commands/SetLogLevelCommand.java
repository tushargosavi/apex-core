package com.datatorrent.stram.cli.commands;

import javax.ws.rs.core.MediaType;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.sun.jersey.api.client.WebResource;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class SetLogLevelCommand implements Command
{

  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String target = args[1];
    String logLevel = args[2];

    StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
    uriSpec = uriSpec.path(StramWebServices.PATH_LOGGERS);
    final JSONObject request = buildRequest(target, logLevel);

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

  private JSONObject buildRequest(String target, String logLevel) throws JSONException
  {
    JSONObject request = new JSONObject();
    JSONArray loggers = new JSONArray();
    JSONObject targetAndLevelPair = new JSONObject();

    targetAndLevelPair.put("target", target);
    targetAndLevelPair.put("logLevel", logLevel);

    loggers.put(targetAndLevelPair);

    request.put("loggers", loggers);

    return request;
  }
}
