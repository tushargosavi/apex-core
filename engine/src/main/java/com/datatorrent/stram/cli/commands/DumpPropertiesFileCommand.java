package com.datatorrent.stram.cli.commands;

import java.io.File;
import java.util.List;

import org.codehaus.jettison.json.JSONObject;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.codec.LogicalPlanSerializer;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class DumpPropertiesFileCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String outfilename = ApexCli.expandFileName(args[1], false);

    if (args.length > 3) {
      String jarfile = args[2];
      String appName = args[3];
      Configuration config = StramClientUtils.addDTSiteResources(new Configuration());
      StramAppLauncher submitApp = apexCli.getStramAppLauncher(jarfile, config, false);
      submitApp.loadDependencies();
      List<StramAppLauncher.AppFactory> matchingAppFactories = apexCli.getMatchingAppFactories(submitApp, appName, true);
      if (matchingAppFactories == null || matchingAppFactories.isEmpty()) {
        submitApp.resetContextClassLoader();
        throw new CliException("No application in jar file matches '" + appName + "'");
      } else if (matchingAppFactories.size() > 1) {
        submitApp.resetContextClassLoader();
        throw new CliException("More than one application in jar file match '" + appName + "'");
      } else {
        StramAppLauncher.AppFactory appFactory = matchingAppFactories.get(0);
        LogicalPlan logicalPlan = appFactory.createApp(submitApp.getLogicalPlanConfiguration());
        File file = new File(outfilename);
        if (!file.exists()) {
          file.createNewFile();
        }
        LogicalPlanSerializer.convertToProperties(logicalPlan).save(file);
        submitApp.resetContextClassLoader();
      }
    } else {
      if (!apexCli.isCurrentApp()) {
        throw new CliException("No application selected");
      }
      JSONObject response = apexCli.getCurrentAppResource(StramWebServices.PATH_LOGICAL_PLAN);
      File file = new File(outfilename);
      if (!file.exists()) {
        file.createNewFile();
      }
      LogicalPlanSerializer.convertToProperties(response).save(file);
    }
    System.out.println("Property file is saved at " + outfilename);
  }
}
