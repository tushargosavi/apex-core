package com.datatorrent.stram.cli.commands;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.client.AppPackage;
import com.datatorrent.stram.codec.LogicalPlanSerializer;

import jline.console.ConsoleReader;

public class ShowLogicalPlanAppPackageCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    try (AppPackage ap = apexCli.newAppPackageInstance(new URI(args[1]), true)) {
      List<AppPackage.AppInfo> applications = ap.getApplications();

      if (args.length >= 3) {
        for (AppPackage.AppInfo appInfo : applications) {
          if (args[2].equals(appInfo.name)) {
            Map<String, Object> map = new HashMap<>();
            map.put("applicationName", appInfo.name);
            if (appInfo.dag != null) {
              map.put("logicalPlan", LogicalPlanSerializer.convertToMap(appInfo.dag, false));
            }
            if (appInfo.error != null) {
              map.put("error", appInfo.error);
            }
            apexCli.printJson(map);
          }
        }
      } else {
        List<Map<String, Object>> appList = new ArrayList<>();
        for (AppPackage.AppInfo appInfo : applications) {
          Map<String, Object> m = new HashMap<>();
          m.put("name", appInfo.name);
          m.put("type", appInfo.type);
          appList.add(m);
        }
        apexCli.printJson(appList, "applications");
      }
    }
  }
}
