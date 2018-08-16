package com.datatorrent.stram.cli.commands;

import java.net.URLEncoder;

import javax.ws.rs.core.MediaType;

import org.codehaus.jettison.json.JSONObject;

import com.sun.jersey.api.client.WebResource;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.plan.logical.requests.SetOperatorPropertyRequest;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class SetOperatorPropertyCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    if (!apexCli.isCurrentApp()) {
      throw new CliException("No application selected");
    }
    if (apexCli.isChangingLogicalPlan()) {
      String operatorName = args[1];
      String propertyName = args[2];
      String propertyValue = args[3];
      SetOperatorPropertyRequest request = new SetOperatorPropertyRequest();
      request.setOperatorName(operatorName);
      request.setPropertyName(propertyName);
      request.setPropertyValue(propertyValue);
      apexCli.getLogicalPlanRequestQueue().add(request);
    } else {
      StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
      uriSpec = uriSpec.path(StramWebServices.PATH_LOGICAL_PLAN_OPERATORS).path(URLEncoder.encode(args[1], "UTF-8")).path("properties");
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
}
