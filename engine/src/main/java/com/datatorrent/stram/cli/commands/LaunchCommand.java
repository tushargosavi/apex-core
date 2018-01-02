/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.cli.commands;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.ClusterProviderFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.cli.util.LaunchCommandLineInfo;
import com.datatorrent.stram.client.AppPackage;
import com.datatorrent.stram.client.ConfigPackage;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.client.StramClientUtils;

import jline.console.ConsoleReader;
import jline.console.completer.Completer;
import jline.console.history.History;
import jline.console.history.MemoryHistory;

public abstract class LaunchCommand implements Command
{
  private static final Logger LOG = LoggerFactory.getLogger(LaunchCommand.class);

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String[] newArgs = new String[args.length - 1];
    System.arraycopy(args, 1, newArgs, 0, args.length - 1);
    LaunchCommandLineInfo commandLineInfo = LaunchCommandLineInfo.getLaunchCommandLineInfo(newArgs);

    if (commandLineInfo.configFile != null) {
      commandLineInfo.configFile = ApexCli.expandFileName(commandLineInfo.configFile, true);
    }

    // see if the given config file is a config package
    ConfigPackage cp = null;
    String requiredAppPackageName = null;
    try {
      cp = new ConfigPackage(new File(commandLineInfo.configFile));
      requiredAppPackageName = cp.getAppPackageName();
    } catch (Exception ex) {
      // fall through, it's not a config package
    }
    try {
      Configuration config;
      String configFile = cp == null ? commandLineInfo.configFile : null;
      try {
        config = StramAppLauncher.getOverriddenConfig(StramClientUtils.addDTSiteResources(new Configuration()), configFile, commandLineInfo.overrideProperties);
        if (commandLineInfo.libjars != null) {
          commandLineInfo.libjars = ApexCli.expandCommaSeparatedFiles(commandLineInfo.libjars);
          if (commandLineInfo.libjars != null) {
            config.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, commandLineInfo.libjars);
          }
        }
        if (commandLineInfo.files != null) {
          commandLineInfo.files = ApexCli.expandCommaSeparatedFiles(commandLineInfo.files);
          if (commandLineInfo.files != null) {
            config.set(StramAppLauncher.FILES_CONF_KEY_NAME, commandLineInfo.files);
          }
        }
        if (commandLineInfo.archives != null) {
          commandLineInfo.archives = ApexCli.expandCommaSeparatedFiles(commandLineInfo.archives);
          if (commandLineInfo.archives != null) {
            config.set(StramAppLauncher.ARCHIVES_CONF_KEY_NAME, commandLineInfo.archives);
          }
        }
        if (commandLineInfo.origAppId != null) {
          config.set(StramAppLauncher.ORIGINAL_APP_ID, commandLineInfo.origAppId);
        }
        config.set(StramAppLauncher.QUEUE_NAME, commandLineInfo.queue != null ? commandLineInfo.queue : "default");
        if (commandLineInfo.tags != null) {
          config.set(StramAppLauncher.TAGS, commandLineInfo.tags);
        }
      } catch (Exception ex) {
        throw new CliException("Error opening the config XML file: " + configFile, ex);
      }
      StramAppLauncher submitApp;
      StramAppLauncher.AppFactory appFactory = null;
      String matchString = null;
      if (commandLineInfo.args.length == 0) {
        if (commandLineInfo.origAppId == null) {
          throw new CliException("Launch requires an APA or JAR file when not resuming a terminated application");
        }
        submitApp = ClusterProviderFactory.getProvider().getStramAppLauncher(apexCli.getFs(), config);
        appFactory = submitApp.new RecoveryAppFactory();
      } else {
        String fileName = ApexCli.expandFileName(commandLineInfo.args[0], true);
        if (commandLineInfo.args.length >= 2) {
          matchString = commandLineInfo.args[1];
        }
        if (fileName.endsWith(".json")) {
          File file = new File(fileName);
          submitApp = ClusterProviderFactory.getProvider().getStramAppLauncher(file.getName(), config);
          appFactory = new StramAppLauncher.JsonFileAppFactory(file);
          if (matchString != null) {
            LOG.warn("Match string \"{}\" is ignored for launching applications specified in JSON", matchString);
          }
        } else if (fileName.endsWith(".properties")) {
          File file = new File(fileName);
          submitApp = ClusterProviderFactory.getProvider().getStramAppLauncher(file.getName(), config);
          appFactory = new StramAppLauncher.PropertyFileAppFactory(file);
          if (matchString != null) {
            LOG.warn("Match string \"{}\" is ignored for launching applications specified in properties file", matchString);
          }
        } else {
          // see if it's an app package
          AppPackage ap = null;
          try {
            ap = apexCli.newAppPackageInstance(new URI(fileName), true);
          } catch (Exception ex) {
            // It's not an app package
            if (requiredAppPackageName != null) {
              throw new CliException("Config package requires an app package name of \"" + requiredAppPackageName + "\"");
            }
          }

          if (ap != null) {
            try {
              if (!commandLineInfo.force) {
                apexCli.checkPlatformCompatible(ap);
                apexCli.checkConfigPackageCompatible(ap, cp);
              }
              apexCli.launchAppPackage(ap, cp, commandLineInfo, reader);
              return;
            } finally {
              IOUtils.closeQuietly(ap);
            }
          }
          submitApp = apexCli.getStramAppLauncher(fileName, config, commandLineInfo.ignorePom);
        }
      }
      submitApp.loadDependencies();

      if (commandLineInfo.origAppId != null) {
        appFactory = checkNonTerminatedApplication(apexCli, commandLineInfo, appFactory, submitApp, matchString);
      }

      if (appFactory == null && matchString != null) {
        // attempt to interpret argument as property file - do we still need it?
        try {
          File file = new File(ApexCli.expandFileName(commandLineInfo.args[1], true));
          if (file.exists()) {
            if (commandLineInfo.args[1].endsWith(".properties")) {
              appFactory = new StramAppLauncher.PropertyFileAppFactory(file);
            } else if (commandLineInfo.args[1].endsWith(".json")) {
              appFactory = new StramAppLauncher.JsonFileAppFactory(file);
            }
          }
        } catch (Exception | NoClassDefFoundError ex) {
          // ignore
        }
      }

      if (appFactory == null) {
        List<StramAppLauncher.AppFactory> matchingAppFactories = apexCli.getMatchingAppFactories(submitApp, matchString, commandLineInfo.exactMatch);
        if (matchingAppFactories == null || matchingAppFactories.isEmpty()) {
          throw new CliException("No applications matching \"" + matchString + "\" bundled in jar.");
        } else if (matchingAppFactories.size() == 1) {
          appFactory = matchingAppFactories.get(0);
        } else if (matchingAppFactories.size() > 1) {

          //Store the appNames sorted in alphabetical order and their position in matchingAppFactories list
          TreeMap<String, Integer> appNamesInAlphabeticalOrder = new TreeMap<>();
          // Display matching applications
          for (int i = 0; i < matchingAppFactories.size(); i++) {
            String appName = matchingAppFactories.get(i).getName();
            String appAlias = submitApp.getLogicalPlanConfiguration().getAppAlias(appName);
            if (appAlias != null) {
              appName = appAlias;
            }
            appNamesInAlphabeticalOrder.put(appName, i);
          }

          //Create a mapping between the app display number and original index at matchingAppFactories
          int index = 1;
          HashMap<Integer, Integer> displayIndexToOriginalUnsortedIndexMap = new HashMap<>();
          for (Map.Entry<String, Integer> entry : appNamesInAlphabeticalOrder.entrySet()) {
            //Map display number of the app to original unsorted index
            displayIndexToOriginalUnsortedIndexMap.put(index, entry.getValue());

            //Display the app names
            System.out.printf("%3d. %s\n", index++, entry.getKey());
          }

          // Exit if not in interactive mode
          if (!apexCli.isConsolePresent()) {
            throw new CliException("More than one application in jar file match '" + matchString + "'");
          } else {

            boolean useHistory = reader.isHistoryEnabled();
            reader.setHistoryEnabled(false);
            History previousHistory = reader.getHistory();
            History dummyHistory = new MemoryHistory();
            reader.setHistory(dummyHistory);
            List<Completer> completers = new ArrayList<>(reader.getCompleters());
            for (Completer c : completers) {
              reader.removeCompleter(c);
            }
            reader.setHandleUserInterrupt(true);
            String optionLine;
            try {
              optionLine = reader.readLine("Choose application: ");
            } finally {
              reader.setHandleUserInterrupt(false);
              reader.setHistoryEnabled(useHistory);
              reader.setHistory(previousHistory);
              for (Completer c : completers) {
                reader.addCompleter(c);
              }
            }
            try {
              int option = Integer.parseInt(optionLine);
              if (0 < option && option <= matchingAppFactories.size()) {
                int appIndex = displayIndexToOriginalUnsortedIndexMap.get(option);
                appFactory = matchingAppFactories.get(appIndex);
              }
            } catch (Exception ex) {
              // ignore
            }
          }
        }
      }

      if (appFactory != null) {
        if (!commandLineInfo.localMode) {
          checkDuplicatesAndLaunchApplication(config, apexCli, appFactory, submitApp);
        } else {
          submitApp.runLocal(appFactory);
        }
      } else {
        System.err.println("No application specified.");
      }
      submitApp.resetContextClassLoader();
    } finally {
      IOUtils.closeQuietly(cp);
    }
  }

  protected abstract StramAppLauncher.AppFactory checkNonTerminatedApplication(ApexCli apexCli,
      LaunchCommandLineInfo commandLineInfo, StramAppLauncher.AppFactory appFactory,
      StramAppLauncher submitApp, String matchString);

  protected abstract void checkDuplicatesAndLaunchApplication(Configuration config, ApexCli apexCli,
      StramAppLauncher.AppFactory appFactory, StramAppLauncher submitApp) throws Exception;
}
