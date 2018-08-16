package com.datatorrent.stram.cli.commands;

import java.io.File;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONObject;

import org.apache.apex.engine.ClusterProviderFactory;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.cli.util.ShowLogicalPlanCommandLineInfo;
import com.datatorrent.stram.client.AppPackage;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.codec.LogicalPlanSerializer;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class ShowLogicalPlanCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String[] newArgs = new String[args.length - 1];
    System.arraycopy(args, 1, newArgs, 0, args.length - 1);
    ShowLogicalPlanCommandLineInfo commandLineInfo = ShowLogicalPlanCommandLineInfo.getShowLogicalPlanCommandLineInfo(newArgs);
    Configuration config = StramClientUtils.addDTSiteResources(new Configuration());
    if (commandLineInfo.libjars != null) {
      commandLineInfo.libjars = apexCli.expandCommaSeparatedFiles(commandLineInfo.libjars);
      if (commandLineInfo.libjars != null) {
        config.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, commandLineInfo.libjars);
      }
    }

    if (commandLineInfo.args.length > 0) {
      // see if the first argument is actually an app package
      try (AppPackage ap = apexCli.newAppPackageInstance(new URI(commandLineInfo.args[0]), false)) {
        new ShowLogicalPlanAppPackageCommand().execute(args, reader, apexCli);
        return;
      } catch (Exception ex) {
        // fall through
      }

      String filename = ApexCli.expandFileName(commandLineInfo.args[0], true);
      if (commandLineInfo.args.length >= 2) {
        String appName = commandLineInfo.args[1];
        StramAppLauncher submitApp = apexCli.getStramAppLauncher(filename, config, commandLineInfo.ignorePom);
        submitApp.loadDependencies();
        List<StramAppLauncher.AppFactory> matchingAppFactories = apexCli.getMatchingAppFactories(submitApp, appName, commandLineInfo.exactMatch);
        if (matchingAppFactories == null || matchingAppFactories.isEmpty()) {
          submitApp.resetContextClassLoader();
          throw new CliException("No application in jar file matches '" + appName + "'");
        } else if (matchingAppFactories.size() > 1) {
          submitApp.resetContextClassLoader();
          throw new CliException("More than one application in jar file match '" + appName + "'");
        } else {
          Map<String, Object> map = new HashMap<>();
          PrintStream originalStream = System.out;
          StramAppLauncher.AppFactory appFactory = matchingAppFactories.get(0);
          try {
            if (apexCli.isRaw()) {
              PrintStream dummyStream = new PrintStream(new OutputStream()
              {
                @Override
                public void write(int b)
                {
                  // no-op
                }

              });
              System.setOut(dummyStream);
            }
            LogicalPlan logicalPlan = appFactory.createApp(submitApp.getLogicalPlanConfiguration());
            map.put("applicationName", appFactory.getName());
            map.put("logicalPlan", LogicalPlanSerializer.convertToMap(logicalPlan, false));
          } finally {
            if (apexCli.isRaw()) {
              System.setOut(originalStream);
            }
          }
          apexCli.printJson(map);
          submitApp.resetContextClassLoader();
        }
      } else {
        if (filename.endsWith(".json")) {
          File file = new File(filename);
          StramAppLauncher submitApp = ClusterProviderFactory.getProvider().getStramAppLauncher(file.getName(), config);
          StramAppLauncher.AppFactory appFactory = new StramAppLauncher.JsonFileAppFactory(file);
          LogicalPlan logicalPlan = appFactory.createApp(submitApp.getLogicalPlanConfiguration());
          Map<String, Object> map = new HashMap<>();
          map.put("applicationName", appFactory.getName());
          map.put("logicalPlan", LogicalPlanSerializer.convertToMap(logicalPlan, false));
          apexCli.printJson(map);
        } else if (filename.endsWith(".properties")) {
          File file = new File(filename);
          StramAppLauncher submitApp = ClusterProviderFactory.getProvider().getStramAppLauncher(file.getName(), config);
          StramAppLauncher.AppFactory appFactory = new StramAppLauncher.PropertyFileAppFactory(file);
          LogicalPlan logicalPlan = appFactory.createApp(submitApp.getLogicalPlanConfiguration());
          Map<String, Object> map = new HashMap<>();
          map.put("applicationName", appFactory.getName());
          map.put("logicalPlan", LogicalPlanSerializer.convertToMap(logicalPlan, false));
          apexCli.printJson(map);
        } else {
          StramAppLauncher submitApp = apexCli.getStramAppLauncher(filename, config, commandLineInfo.ignorePom);
          submitApp.loadDependencies();
          List<Map<String, Object>> appList = new ArrayList<>();
          List<StramAppLauncher.AppFactory> appFactoryList = submitApp.getBundledTopologies();
          for (StramAppLauncher.AppFactory appFactory : appFactoryList) {
            Map<String, Object> m = new HashMap<>();
            m.put("name", appFactory.getName());
            appList.add(m);
          }
          apexCli.printJson(appList, "applications");
          submitApp.resetContextClassLoader();
        }
      }
    } else {
      if (!apexCli.isCurrentApp()) {
        throw new CliException("No application selected");
      }
      JSONObject response = apexCli.getCurrentAppResource(StramWebServices.PATH_LOGICAL_PLAN);
      apexCli.printJson(response);
    }
  }
}
