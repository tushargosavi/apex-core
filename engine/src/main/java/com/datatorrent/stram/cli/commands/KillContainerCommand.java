package com.datatorrent.stram.cli.commands;

import java.net.URLEncoder;

import javax.ws.rs.core.MediaType;

import org.codehaus.jettison.json.JSONObject;

import com.sun.jersey.api.client.WebResource;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class KillContainerCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    for (int i = 1; i < args.length; i++) {
      String containerLongId = apexCli.getContainerLongId(args[i]);
      if (containerLongId == null) {
        throw new CliException("Container " + args[i] + " not found");
      }
      try {
        StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
        uriSpec = uriSpec.path(StramWebServices.PATH_PHYSICAL_PLAN_CONTAINERS).path(URLEncoder.encode(containerLongId, "UTF-8")).path("kill");
        JSONObject response = apexCli.getCurrentAppResource(uriSpec, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).post(clazz, new JSONObject());
          }

        });
        if (apexCli.isConsolePresent()) {
          System.out.println("Kill container requested: " + response);
        }
      } catch (Exception e) {
        throw new CliException("Failed web service request for appid " + apexCli.getCurrentAppId(), e);
      }
    }
  }
}
