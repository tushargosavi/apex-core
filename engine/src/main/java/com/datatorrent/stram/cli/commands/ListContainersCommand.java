package com.datatorrent.stram.cli.commands;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class ListContainersCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    JSONObject json = apexCli.getCurrentAppResource(StramWebServices.PATH_PHYSICAL_PLAN_CONTAINERS);
    if (args.length == 1) {
      apexCli.printJson(json);
    } else {
      Object containersObj = json.get("containers");
      JSONArray containers;
      if (containersObj instanceof JSONArray) {
        containers = (JSONArray)containersObj;
      } else {
        containers = new JSONArray();
        containers.put(containersObj);
      }
      if (containersObj == null) {
        System.out.println("No containers found!");
      } else {
        JSONArray resultContainers = new JSONArray();
        for (int o = containers.length(); o-- > 0; ) {
          JSONObject container = containers.getJSONObject(o);
          String id = container.getString("id");
          if (id != null && !id.isEmpty()) {
            for (int argc = args.length; argc-- > 1; ) {
              String s1 = "0" + args[argc];
              String s2 = "_" + args[argc];
              if (id.equals(args[argc]) || id.endsWith(s1) || id.endsWith(s2)) {
                resultContainers.put(container);
              }
            }
          }
        }
        apexCli.printJson(resultContainers, "containers");
      }
    }
  }
}
