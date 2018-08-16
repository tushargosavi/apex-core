package com.datatorrent.stram.cli.commands;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONObject;

import com.sun.jersey.api.client.WebResource;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class SubmitCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    if (apexCli.getLogicalPlanRequestQueue().isEmpty()) {
      throw new CliException("Nothing to submit. Type \"abort\" to abort change");
    }
    StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
    uriSpec = uriSpec.path(StramWebServices.PATH_LOGICAL_PLAN);
    try {
      final Map<String, Object> m = new HashMap<>();
      ObjectMapper mapper = new ObjectMapper();
      m.put("requests", apexCli.getLogicalPlanRequestQueue());
      final JSONObject jsonRequest = new JSONObject(mapper.writeValueAsString(m));

      JSONObject response = apexCli.getCurrentAppResource(uriSpec, new WebServicesClient.WebServicesHandler<JSONObject>()
      {
        @Override
        public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
        {
          return webResource.accept(MediaType.APPLICATION_JSON).post(JSONObject.class, jsonRequest);
        }

      });
      apexCli.printJson(response);
    } catch (Exception e) {
      throw new CliException("Failed web service request for appid " + apexCli.getCurrentAppId(), e);
    }
    apexCli.getLogicalPlanRequestQueue().clear();
    apexCli.setChangingLogicalPlan(false);
    reader.setHistory(apexCli.getTopLevelHistory());
  }
}

