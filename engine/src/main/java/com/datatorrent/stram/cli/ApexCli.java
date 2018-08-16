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
package com.datatorrent.stram.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.ClusterProviderFactory;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.tools.ant.DirectoryScanner;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.cli.commands.AbortCommand;
import com.datatorrent.stram.cli.commands.AddStreamSinkCommand;
import com.datatorrent.stram.cli.commands.AliasCommand;
import com.datatorrent.stram.cli.commands.BeginLogicalPlanChangeCommand;
import com.datatorrent.stram.cli.commands.BeginMacroCommand;
import com.datatorrent.stram.cli.commands.Command;
import com.datatorrent.stram.cli.commands.ConnectCommand;
import com.datatorrent.stram.cli.commands.CreateOperatorCommand;
import com.datatorrent.stram.cli.commands.CreateStreamCommand;
import com.datatorrent.stram.cli.commands.DumpPropertiesFileCommand;
import com.datatorrent.stram.cli.commands.EchoCommand;
import com.datatorrent.stram.cli.commands.ExitCommand;
import com.datatorrent.stram.cli.commands.GetAppAttributesCommand;
import com.datatorrent.stram.cli.commands.GetAppPackageInfoCommand;
import com.datatorrent.stram.cli.commands.GetAppPackageOperatorPropertiesCommand;
import com.datatorrent.stram.cli.commands.GetAppPackageOperatorsCommand;
import com.datatorrent.stram.cli.commands.GetConfigParameterCommand;
import com.datatorrent.stram.cli.commands.GetContainerStackTrace;
import com.datatorrent.stram.cli.commands.GetJarOperatorClassesCommand;
import com.datatorrent.stram.cli.commands.GetJarOperatorPropertiesCommand;
import com.datatorrent.stram.cli.commands.GetOperatorAttributesCommand;
import com.datatorrent.stram.cli.commands.GetOperatorPropertiesCommand;
import com.datatorrent.stram.cli.commands.GetPhysicalOperatorPropertiesCommand;
import com.datatorrent.stram.cli.commands.GetPortAttributesCommand;
import com.datatorrent.stram.cli.commands.GetRecordingInfoCommand;
import com.datatorrent.stram.cli.commands.HelpCommand;
import com.datatorrent.stram.cli.commands.KillContainerCommand;
import com.datatorrent.stram.cli.commands.ListContainersCommand;
import com.datatorrent.stram.cli.commands.ListDefaultAttributesCommand;
import com.datatorrent.stram.cli.commands.ListOperatorsCommand;
import com.datatorrent.stram.cli.commands.RemoveOperatorCommand;
import com.datatorrent.stram.cli.commands.RemoveStreamCommand;
import com.datatorrent.stram.cli.commands.SetLogLevelCommand;
import com.datatorrent.stram.cli.commands.SetOperatorAttributeCommand;
import com.datatorrent.stram.cli.commands.SetOperatorPropertyCommand;
import com.datatorrent.stram.cli.commands.SetPagerCommand;
import com.datatorrent.stram.cli.commands.SetPhysicalOperatorPropertyCommand;
import com.datatorrent.stram.cli.commands.SetPortAttributeCommand;
import com.datatorrent.stram.cli.commands.SetStreamAttributeCommand;
import com.datatorrent.stram.cli.commands.ShowLogicalPlanCommand;
import com.datatorrent.stram.cli.commands.ShowPhysicalPlanCommand;
import com.datatorrent.stram.cli.commands.ShowQueueCommand;
import com.datatorrent.stram.cli.commands.SourceCommand;
import com.datatorrent.stram.cli.commands.StartRecordingCommand;
import com.datatorrent.stram.cli.commands.StopRecordingCommand;
import com.datatorrent.stram.cli.commands.SubmitCommand;
import com.datatorrent.stram.cli.util.Arg;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.cli.util.CommandArg;
import com.datatorrent.stram.cli.util.CommandSpec;
import com.datatorrent.stram.cli.util.FileArg;
import com.datatorrent.stram.cli.util.FileLineReader;
import com.datatorrent.stram.cli.util.GetAppPackageInfoCommandLineInfo;
import com.datatorrent.stram.cli.util.GetOperatorClassesCommandLineInfo;
import com.datatorrent.stram.cli.util.GetPhysicalPropertiesCommandLineOptions;
import com.datatorrent.stram.cli.util.LaunchCommandLineInfo;
import com.datatorrent.stram.cli.util.MyFileNameCompleter;
import com.datatorrent.stram.cli.util.MyNullCompleter;
import com.datatorrent.stram.cli.util.OptionsCommandSpec;
import com.datatorrent.stram.cli.util.ShowLogicalPlanCommandLineInfo;
import com.datatorrent.stram.cli.util.Tokenizer;
import com.datatorrent.stram.cli.util.VarArg;
import com.datatorrent.stram.client.AppPackage;
import com.datatorrent.stram.client.AppPackage.AppInfo;
import com.datatorrent.stram.client.AppPackage.PropertyInfo;
import com.datatorrent.stram.client.ConfigPackage;
import com.datatorrent.stram.client.DTConfiguration;
import com.datatorrent.stram.client.DTConfiguration.Scope;
import com.datatorrent.stram.client.RecordingsAgent;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.client.StramAppLauncher.AppFactory;
import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.plan.logical.requests.LogicalPlanRequest;
import com.datatorrent.stram.security.StramUserLogin;
import com.datatorrent.stram.util.JSONSerializationProvider;
import com.datatorrent.stram.util.LoggerUtil;
import com.datatorrent.stram.util.SecurityUtils;
import com.datatorrent.stram.util.VersionInfo;
import com.datatorrent.stram.util.WebServicesClient;

import jline.console.ConsoleReader;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;
import jline.console.history.History;
import jline.console.history.MemoryHistory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * Provides command line interface for a streaming application on hadoop (yarn)
 * <p>
 *
 * @since 0.3.2
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public abstract class ApexCli
{
  private static final Logger LOG = LoggerFactory.getLogger(ApexCli.class);
  private static String CONFIG_EXCLUSIVE = "exclusive";
  private static String CONFIG_INCLUSIVE = "inclusive";

  private static final String COLOR_RED = "\033[38;5;196m";
  private static final String COLOR_YELLOW = "\033[0;93m";
  private static final String FORMAT_BOLD = "\033[1m";

  private static final String COLOR_RESET = "\033[0m";
  private static final String ITALICS = "\033[3m";
  private static final String APEX_HIGHLIGHT_COLOR_PROPERTY_NAME = "apex.cli.color.highlight";
  private static final String APEX_HIGHLIGHT_COLOR_ENV_VAR_NAME = "APEX_HIGHLIGHT_COLOR";

  protected Configuration conf;
  protected FileSystem fs;
  protected StramAgent stramAgent;
  private boolean consolePresent;
  protected String[] commandsToExecute;
  protected final Map<String, CommandSpec> globalCommands = new TreeMap<>();
  protected final Map<String, CommandSpec> connectedCommands = new TreeMap<>();
  protected final Map<String, CommandSpec> logicalPlanChangeCommands = new TreeMap<>();
  protected final Map<String, String> aliases = new HashMap<>();
  protected final Map<String, List<String>> macros = new HashMap<>();
  protected boolean changingLogicalPlan = false;
  protected final List<LogicalPlanRequest> logicalPlanRequestQueue = new ArrayList<>();
  protected FileHistory topLevelHistory;
  protected FileHistory changingLogicalPlanHistory;
  protected String jsonp;
  protected boolean raw = false;
  protected RecordingsAgent recordingsAgent;
  protected final ObjectMapper mapper = new JSONSerializationProvider().getContext(null);
  protected String pagerCommand;
  protected Process pagerProcess;
  protected int verboseLevel = 0;
  protected final Tokenizer tokenizer = new Tokenizer(this);
  protected final Map<String, String> variableMap = new HashMap<>();
  private static boolean lastCommandError = false;
  protected Thread mainThread;
  protected Thread commandThread;
  protected String prompt;
  protected String forcePrompt;
  protected String kerberosPrincipal;
  protected String kerberosKeyTab;
  protected String highlightColor = null;

  public Configuration getConf()
  {
    return conf;
  }

  public FileSystem getFs()
  {
    return fs;
  }

  public boolean isConsolePresent()
  {
    return consolePresent;
  }

  public Map<String, CommandSpec> getGlobalCommands()
  {
    return globalCommands;
  }

  public Map<String, CommandSpec> getConnectedCommands()
  {
    return connectedCommands;
  }

  public Map<String, CommandSpec> getLogicalPlanChangeCommands()
  {
    return logicalPlanChangeCommands;
  }

  public Map<String, String> getAliases()
  {
    return aliases;
  }

  public Map<String, List<String>> getMacros()
  {
    return macros;
  }

  public boolean isChangingLogicalPlan()
  {
    return changingLogicalPlan;
  }

  public List<LogicalPlanRequest> getLogicalPlanRequestQueue()
  {
    return logicalPlanRequestQueue;
  }

  public FileHistory getTopLevelHistory()
  {
    return topLevelHistory;
  }

  public FileHistory getChangingLogicalPlanHistory()
  {
    return changingLogicalPlanHistory;
  }

  public boolean isRaw()
  {
    return raw;
  }

  public RecordingsAgent getRecordingsAgent()
  {
    return recordingsAgent;
  }

  public ObjectMapper getMapper()
  {
    return mapper;
  }

  public Map<String, String> getVariableMap()
  {
    return variableMap;
  }

  public static boolean isLastCommandError()
  {
    return lastCommandError;
  }

  public void setChangingLogicalPlan(boolean changingLogicalPlan)
  {
    this.changingLogicalPlan = changingLogicalPlan;
  }

  public void setPagerCommand(String pagerCommand)
  {
    this.pagerCommand = pagerCommand;
  }

  public PrintStream suppressOutput()
  {
    PrintStream originalStream = System.out;
    if (raw) {
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
    return originalStream;
  }

  public void restoreOutput(PrintStream originalStream)
  {
    if (raw) {
      System.setOut(originalStream);
    }
  }

  public AppPackage newAppPackageInstance(URI uri, boolean suppressOutput) throws IOException
  {
    PrintStream outputStream = suppressOutput ? suppressOutput() : null;
    try {
      final String scheme = uri.getScheme();
      if (scheme == null || scheme.equals("file")) {
        return new AppPackage(new FileInputStream(new File(expandFileName(uri.getPath(), true))), true);
      } else {
        try (FileSystem fs = FileSystem.newInstance(uri, conf)) {
          return new AppPackage(fs.open(new Path(uri.getPath())), true);
        }
      }
    } finally {
      if (outputStream != null) {
        restoreOutput(outputStream);
      }
    }
  }

  AppPackage newAppPackageInstance(File f) throws IOException
  {
    PrintStream outputStream = suppressOutput();
    try {
      return new AppPackage(f, true);
    } finally {
      restoreOutput(outputStream);
    }
  }

  @SuppressWarnings("unused")
  public StramAppLauncher getStramAppLauncher(String jarfileUri, Configuration config, boolean ignorePom) throws Exception
  {
    URI uri = new URI(jarfileUri);
    String scheme = uri.getScheme();
    StramAppLauncher appLauncher = null;
    if (scheme == null || scheme.equals("file")) {
      File jf = new File(uri.getPath());
      appLauncher = ClusterProviderFactory.getProvider().getStramAppLauncher(jf, config);
    } else {
      try (FileSystem tmpFs = FileSystem.newInstance(uri, conf)) {
        Path path = new Path(uri.getPath());
        appLauncher = ClusterProviderFactory.getProvider().getStramAppLauncher(tmpFs, path, config);
      }
    }
    if (appLauncher != null) {
      if (verboseLevel > 0) {
        System.err.print(appLauncher.getMvnBuildClasspathOutput());
      }
      return appLauncher;
    } else {
      throw new CliException("Scheme " + scheme + " not supported.");
    }
  }

  public abstract JSONObject getCurrentAppResource(String resourcePath);

  public abstract JSONObject getCurrentAppResource(StramAgent.StramUriSpec uriSpec);

  public abstract JSONObject getCurrentAppResource(StramAgent.StramUriSpec uriSpec, WebServicesClient.WebServicesHandler handler);

  public abstract String getCurrentAppId();

  public abstract void stop();

  protected abstract Command getCleanAppDirectoriesCommand();

  protected abstract Command getGetAppInfoCommand();

  protected abstract Command getKillAppCommand();

  protected abstract Command getLaunchCommand();

  protected abstract Command getListAppsCommand();

  protected abstract Command getShutdownAppCommand();

  protected abstract Command getWaitCommand();

  protected ApexCli()
  {
    //
    // Global command specification starts here
    //
    globalCommands.put("help", new CommandSpec(new HelpCommand(),
        null,
        new Arg[]{new CommandArg("command")},
        "Show help"));
    globalCommands.put("echo", new CommandSpec(new EchoCommand(),
        null, new Arg[]{new VarArg("arg")},
        "Echo the arguments"));
    globalCommands.put("connect", new CommandSpec(new ConnectCommand(),
        new Arg[]{new Arg("app-id")},
        null,
        "Connect to an app"));
    globalCommands.put("launch", new OptionsCommandSpec(getLaunchCommand(),
        new Arg[]{},
        new Arg[]{new FileArg("jar-file/json-file/properties-file/app-package-file-path/app-package-file-uri"), new Arg("matching-app-name")},
        "Launch an app", LaunchCommandLineInfo.LAUNCH_OPTIONS.options));
    globalCommands.put("shutdown-app", new CommandSpec(getShutdownAppCommand(),
        new Arg[]{new Arg("app-id/app-name")},
        new Arg[]{new VarArg("app-id/app-name")},
        "Shutdown application(s) by id or name"));
    globalCommands.put("list-apps", new CommandSpec(getListAppsCommand(),
        null,
        new Arg[]{new Arg("pattern")},
        "List applications"));
    globalCommands.put("kill-app", new CommandSpec(getKillAppCommand(),
        new Arg[]{new Arg("app-id/app-name")},
        new Arg[]{new VarArg("app-id/app-name")},
        "Kill an app"));
    globalCommands.put("show-logical-plan", new OptionsCommandSpec(new ShowLogicalPlanCommand(),
        new Arg[]{new FileArg("jar-file/app-package-file")},
        new Arg[]{new Arg("class-name")},
        "List apps in a jar or show logical plan of an app class",
        ShowLogicalPlanCommandLineInfo.getShowLogicalPlanCommandLineOptions()));

    globalCommands.put("get-jar-operator-classes", new OptionsCommandSpec(new GetJarOperatorClassesCommand(),
        new Arg[]{new FileArg("jar-files-comma-separated")},
        new Arg[]{new Arg("search-term")},
        "List operators in a jar list",
        GetOperatorClassesCommandLineInfo.GET_OPERATOR_CLASSES_OPTIONS.options));

    globalCommands.put("get-jar-operator-properties", new CommandSpec(new GetJarOperatorPropertiesCommand(),
        new Arg[]{new FileArg("jar-files-comma-separated"), new Arg("operator-class-name")},
        null,
        "List properties in specified operator"));

    globalCommands.put("alias", new CommandSpec(new AliasCommand(),
        new Arg[]{new Arg("alias-name"), new CommandArg("command")},
        null,
        "Create a command alias"));
    globalCommands.put("source", new CommandSpec(new SourceCommand(),
        new Arg[]{new FileArg("file")},
        null,
        "Execute the commands in a file"));
    globalCommands.put("exit", new CommandSpec(new ExitCommand(),
        null,
        null,
        "Exit the CLI"));
    globalCommands.put("begin-macro", new CommandSpec(new BeginMacroCommand(),
        new Arg[]{new Arg("name")},
        null,
        "Begin Macro Definition ($1...$9 to access parameters and type 'end' to end the definition)"));
    globalCommands.put("dump-properties-file", new CommandSpec(new DumpPropertiesFileCommand(),
        new Arg[]{new FileArg("out-file"), new FileArg("jar-file"), new Arg("app-name")},
        null,
        "Dump the properties file of an app class"));
    globalCommands.put("get-app-info", new CommandSpec(getGetAppInfoCommand(),
        new Arg[]{new Arg("app-id")},
        null,
        "Get the information of an app"));
    globalCommands.put("set-pager", new CommandSpec(new SetPagerCommand(),
        new Arg[]{new Arg("on/off")},
        null,
        "Set the pager program for output"));
    globalCommands.put("get-config-parameter", new CommandSpec(new GetConfigParameterCommand(),
        null,
        new Arg[]{new FileArg("parameter-name")},
        "Get the configuration parameter"));
    globalCommands.put("get-app-package-info", new OptionsCommandSpec(new GetAppPackageInfoCommand(),
        new Arg[]{new FileArg("app-package-file-path/app-package-file-uri")},
        new Arg[]{new Arg("-withDescription")},
        "Get info on the app package file",
        GetAppPackageInfoCommandLineInfo.GET_APP_PACKAGE_INFO_OPTIONS));
    globalCommands.put("get-app-package-operators", new OptionsCommandSpec(new GetAppPackageOperatorsCommand(),
        new Arg[]{new FileArg("app-package-file-path/app-package-file-uri")},
        new Arg[]{new Arg("search-term")},
        "Get operators within the given app package",
        GetOperatorClassesCommandLineInfo.GET_OPERATOR_CLASSES_OPTIONS.options));
    globalCommands.put("get-app-package-operator-properties", new CommandSpec(new GetAppPackageOperatorPropertiesCommand(),
        new Arg[]{new FileArg("app-package-file-path/app-package-file-uri"), new Arg("operator-class")},
        null,
        "Get operator properties within the given app package"));
    globalCommands.put("list-default-app-attributes", new CommandSpec(new ListDefaultAttributesCommand(AttributesType.APPLICATION),
        null, null, "Lists the default application attributes"));
    globalCommands.put("list-default-operator-attributes", new CommandSpec(new ListDefaultAttributesCommand(AttributesType.OPERATOR),
        null, null, "Lists the default operator attributes"));
    globalCommands.put("list-default-port-attributes", new CommandSpec(new ListDefaultAttributesCommand(AttributesType.PORT),
        null, null, "Lists the default port attributes"));
    globalCommands.put("clean-app-directories", new CommandSpec(getCleanAppDirectoriesCommand(),
        new Arg[]{new Arg("duration-in-millis")},
        null,
        "Clean up data directories of applications that terminated the given milliseconds ago"));

    //
    // Connected command specification starts here
    //
    connectedCommands.put("list-containers", new CommandSpec(new ListContainersCommand(),
        null,
        null,
        "List containers"));
    connectedCommands.put("list-operators", new CommandSpec(new ListOperatorsCommand(),
        null,
        new Arg[]{new Arg("pattern")},
        "List operators"));
    connectedCommands.put("show-physical-plan", new CommandSpec(new ShowPhysicalPlanCommand(),
        null,
        null,
        "Show physical plan"));
    connectedCommands.put("kill-container", new CommandSpec(new KillContainerCommand(),
        new Arg[]{new Arg("container-id")},
        new Arg[]{new VarArg("container-id")},
        "Kill a container"));
    connectedCommands.put("shutdown-app", new CommandSpec(getShutdownAppCommand(),
        null,
        new Arg[]{new VarArg("app-id/app-name")},
        "Shutdown an app"));
    connectedCommands.put("kill-app", new CommandSpec(getKillAppCommand(),
        null,
        new Arg[]{new VarArg("app-id/app-name")},
        "Kill an app"));
    connectedCommands.put("wait", new CommandSpec(getWaitCommand(),
        new Arg[]{new Arg("timeout")},
        null,
        "Wait for completion of current application"));
    connectedCommands.put("start-recording", new CommandSpec(new StartRecordingCommand(),
        new Arg[]{new Arg("operator-id")},
        new Arg[]{new Arg("port-name"), new Arg("num-windows")},
        "Start recording"));
    connectedCommands.put("stop-recording", new CommandSpec(new StopRecordingCommand(),
        new Arg[]{new Arg("operator-id")},
        new Arg[]{new Arg("port-name")},
        "Stop recording"));
    connectedCommands.put("get-operator-attributes", new CommandSpec(new GetOperatorAttributesCommand(),
        new Arg[]{new Arg("operator-name")},
        new Arg[]{new Arg("attribute-name")},
        "Get attributes of an operator"));
    connectedCommands.put("get-operator-properties", new CommandSpec(new GetOperatorPropertiesCommand(),
        new Arg[]{new Arg("operator-name")},
        new Arg[]{new Arg("property-name")},
        "Get properties of a logical operator"));
    connectedCommands.put("get-physical-operator-properties", new OptionsCommandSpec(new GetPhysicalOperatorPropertiesCommand(),
        new Arg[]{new Arg("operator-id")},
        null,
        "Get properties of a physical operator", GetPhysicalPropertiesCommandLineOptions.GET_PHYSICAL_PROPERTY_OPTIONS.options));

    connectedCommands.put("set-operator-property", new CommandSpec(new SetOperatorPropertyCommand(),
        new Arg[]{new Arg("operator-name"), new Arg("property-name"), new Arg("property-value")},
        null,
        "Set a property of an operator"));
    connectedCommands.put("set-physical-operator-property", new CommandSpec(new SetPhysicalOperatorPropertyCommand(),
        new Arg[]{new Arg("operator-id"), new Arg("property-name"), new Arg("property-value")},
        null,
        "Set a property of an operator"));
    connectedCommands.put("get-app-attributes", new CommandSpec(new GetAppAttributesCommand(),
        null,
        new Arg[]{new Arg("attribute-name")},
        "Get attributes of the connected app"));
    connectedCommands.put("get-port-attributes", new CommandSpec(new GetPortAttributesCommand(),
        new Arg[]{new Arg("operator-name"), new Arg("port-name")},
        new Arg[]{new Arg("attribute-name")},
        "Get attributes of a port"));
    connectedCommands.put("begin-logical-plan-change", new CommandSpec(new BeginLogicalPlanChangeCommand(),
        null,
        null,
        "Begin Logical Plan Change"));
    connectedCommands.put("show-logical-plan", new OptionsCommandSpec(new ShowLogicalPlanCommand(),
        null,
        new Arg[]{new FileArg("jar-file/app-package-file-path/app-package-file-uri"), new Arg("class-name")},
        "Show logical plan of an app class",
        ShowLogicalPlanCommandLineInfo.getShowLogicalPlanCommandLineOptions()));
    connectedCommands.put("dump-properties-file", new CommandSpec(new DumpPropertiesFileCommand(),
        new Arg[]{new FileArg("out-file")},
        new Arg[]{new FileArg("jar-file"), new Arg("class-name")},
        "Dump the properties file of an app class"));
    connectedCommands.put("get-app-info", new CommandSpec(getGetAppInfoCommand(),
        null,
        new Arg[]{new Arg("app-id")},
        "Get the information of an app"));
    connectedCommands.put("get-recording-info", new CommandSpec(new GetRecordingInfoCommand(),
        null,
        new Arg[]{new Arg("operator-id"), new Arg("start-time")},
        "Get tuple recording info"));
    connectedCommands.put("get-container-stacktrace", new CommandSpec(new GetContainerStackTrace(),
        null,
        new Arg[]{new Arg("container-id")},
        "Get the stack trace for the container"));
    connectedCommands.put("set-log-level", new CommandSpec(new SetLogLevelCommand(),
        new Arg[]{new Arg("target"), new Arg("logLevel")},
        null,
        "Set the logging level of any package or class of the connected app instance"));

    //
    // Logical plan change command specification starts here
    //
    logicalPlanChangeCommands.put("help", new CommandSpec(new HelpCommand(),
        null,
        new Arg[]{new Arg("command")},
        "Show help"));
    logicalPlanChangeCommands.put("create-operator", new CommandSpec(new CreateOperatorCommand(),
        new Arg[]{new Arg("operator-name"), new Arg("class-name")},
        null,
        "Create an operator"));
    logicalPlanChangeCommands.put("create-stream", new CommandSpec(new CreateStreamCommand(),
        new Arg[]{new Arg("stream-name"), new Arg("from-operator-name"), new Arg("from-port-name"), new Arg("to-operator-name"), new Arg("to-port-name")},
        null,
        "Create a stream"));
    logicalPlanChangeCommands.put("add-stream-sink", new CommandSpec(new AddStreamSinkCommand(),
        new Arg[]{new Arg("stream-name"), new Arg("to-operator-name"), new Arg("to-port-name")},
        null,
        "Add a sink to an existing stream"));
    logicalPlanChangeCommands.put("remove-operator", new CommandSpec(new RemoveOperatorCommand(),
        new Arg[]{new Arg("operator-name")},
        null,
        "Remove an operator"));
    logicalPlanChangeCommands.put("remove-stream", new CommandSpec(new RemoveStreamCommand(),
        new Arg[]{new Arg("stream-name")},
        null,
        "Remove a stream"));
    logicalPlanChangeCommands.put("set-operator-property", new CommandSpec(new SetOperatorPropertyCommand(),
        new Arg[]{new Arg("operator-name"), new Arg("property-name"), new Arg("property-value")},
        null,
        "Set a property of an operator"));
    logicalPlanChangeCommands.put("set-operator-attribute", new CommandSpec(new SetOperatorAttributeCommand(),
        new Arg[]{new Arg("operator-name"), new Arg("attr-name"), new Arg("attr-value")},
        null,
        "Set an attribute of an operator"));
    logicalPlanChangeCommands.put("set-port-attribute", new CommandSpec(new SetPortAttributeCommand(),
        new Arg[]{new Arg("operator-name"), new Arg("port-name"), new Arg("attr-name"), new Arg("attr-value")},
        null,
        "Set an attribute of a port"));
    logicalPlanChangeCommands.put("set-stream-attribute", new CommandSpec(new SetStreamAttributeCommand(),
        new Arg[]{new Arg("stream-name"), new Arg("attr-name"), new Arg("attr-value")},
        null,
        "Set an attribute of a stream"));
    logicalPlanChangeCommands.put("show-queue", new CommandSpec(new ShowQueueCommand(),
        null,
        null,
        "Show the queue of the plan change"));
    logicalPlanChangeCommands.put("submit", new CommandSpec(new SubmitCommand(),
        null,
        null,
        "Submit the plan change"));
    logicalPlanChangeCommands.put("abort", new CommandSpec(new AbortCommand(),
        null,
        null,
        "Abort the plan change"));
  }

  public void printJson(String json) throws IOException
  {
    PrintStream os = getOutputPrintStream();

    if (jsonp != null) {
      os.println(jsonp + "(" + json + ");");
    } else {
      os.println(json);
    }
    os.flush();
    closeOutputPrintStream(os);
  }

  public void printJson(JSONObject json) throws JSONException, IOException
  {
    printJson(raw ? json.toString() : json.toString(2));
  }

  public void printJson(JSONArray jsonArray, String name) throws JSONException, IOException
  {
    JSONObject json = new JSONObject();
    json.put(name, jsonArray);
    printJson(json);
  }

  public <K, V> void printJson(Map<K, V> map) throws IOException, JSONException
  {
    printJson(new JSONObject(mapper.writeValueAsString(map)));
  }

  public <T> void printJson(List<T> list, String name) throws IOException, JSONException
  {
    printJson(new JSONArray(mapper.writeValueAsString(list)), name);
  }

  public PrintStream getOutputPrintStream() throws IOException
  {
    if (pagerCommand == null) {
      pagerProcess = null;
      return System.out;
    } else {
      pagerProcess = Runtime.getRuntime().exec(new String[]{"sh", "-c",
        pagerCommand + " >/dev/tty"});
      return new PrintStream(pagerProcess.getOutputStream());
    }
  }

  public void closeOutputPrintStream(PrintStream os)
  {
    if (os != System.out) {
      os.close();
      try {
        pagerProcess.waitFor();
      } catch (InterruptedException ex) {
        LOG.debug("Interrupted");
      }
    }
  }

  public static String expandFileName(String fileName, boolean expandWildCard) throws IOException
  {
    if (fileName.matches("^[a-zA-Z]+:.*")) {
      // it's a URL
      return fileName;
    }

    // TODO: need to work with other users' home directory
    if (fileName.startsWith("~" + File.separator)) {
      fileName = System.getProperty("user.home") + fileName.substring(1);
    }
    fileName = new File(fileName).getCanonicalPath();
    //LOG.debug("Canonical path: {}", fileName);
    if (expandWildCard) {
      DirectoryScanner scanner = new DirectoryScanner();
      scanner.setIncludes(new String[]{fileName});
      scanner.scan();
      String[] files = scanner.getIncludedFiles();

      if (files.length == 0) {
        throw new CliException(fileName + " does not match any file");
      } else if (files.length > 1) {
        throw new CliException(fileName + " matches more than one file");
      }
      return files[0];
    } else {
      return fileName;
    }
  }

  private static String[] expandFileNames(String fileName) throws IOException
  {
    // TODO: need to work with other users
    if (fileName.matches("^[a-zA-Z]+:.*")) {
      // it's a URL
      return new String[]{fileName};
    }
    if (fileName.startsWith("~" + File.separator)) {
      fileName = System.getProperty("user.home") + fileName.substring(1);
    }
    fileName = new File(fileName).getCanonicalPath();
    LOG.debug("Canonical path: {}", fileName);
    DirectoryScanner scanner = new DirectoryScanner();
    scanner.setIncludes(new String[]{fileName});
    scanner.scan();
    return scanner.getIncludedFiles();
  }

  public static String expandCommaSeparatedFiles(String filenames) throws IOException
  {
    String[] entries = filenames.split(",");
    StringBuilder result = new StringBuilder(filenames.length());
    for (String entry : entries) {
      for (String file : expandFileNames(entry)) {
        if (result.length() > 0) {
          result.append(",");
        }
        result.append(file);
      }
    }
    if (result.length() == 0) {
      return null;
    }
    return result.toString();
  }


  public void preImpersonationInit(String[] args) throws IOException
  {
    Signal.handle(new Signal("INT"), new SignalHandler()
    {
      @Override
      public void handle(Signal sig)
      {
        System.out.println("^C");
        if (commandThread != null) {
          commandThread.interrupt();
          mainThread.interrupt();
        } else {
          System.out.print(prompt);
          System.out.flush();
        }
      }
    });
    consolePresent = (System.console() != null);
    Options options = new Options();
    options.addOption("e", true, "Commands are read from the argument");
    options.addOption("v", false, "Verbose mode level 1");
    options.addOption("vv", false, "Verbose mode level 2");
    options.addOption("vvv", false, "Verbose mode level 3");
    options.addOption("vvvv", false, "Verbose mode level 4");
    options.addOption("r", false, "JSON Raw mode");
    options.addOption("p", true, "JSONP padding function");
    options.addOption("h", false, "Print this help");
    options.addOption("f", true, "Use the specified prompt at all time");
    options.addOption("kp", true, "Use the specified kerberos principal");
    options.addOption("kt", true, "Use the specified kerberos keytab");

    CommandLineParser parser = new BasicParser();
    try {
      CommandLine cmd = parser.parse(options, args);
      if (cmd.hasOption("v")) {
        verboseLevel = 1;
      }
      if (cmd.hasOption("vv")) {
        verboseLevel = 2;
      }
      if (cmd.hasOption("vvv")) {
        verboseLevel = 3;
      }
      if (cmd.hasOption("vvvv")) {
        verboseLevel = 4;
      }
      if (cmd.hasOption("r")) {
        raw = true;
      }
      if (cmd.hasOption("e")) {
        commandsToExecute = cmd.getOptionValues("e");
        consolePresent = false;
      }
      if (cmd.hasOption("p")) {
        jsonp = cmd.getOptionValue("p");
      }
      if (cmd.hasOption("f")) {
        forcePrompt = cmd.getOptionValue("f");
      }
      if (cmd.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(ApexCli.class.getSimpleName(), options);
        System.exit(0);
      }
      if (cmd.hasOption("kp")) {
        kerberosPrincipal = cmd.getOptionValue("kp");
      }
      if (cmd.hasOption("kt")) {
        kerberosKeyTab = cmd.getOptionValue("kt");
      }
    } catch (ParseException ex) {
      System.err.println("Invalid argument: " + ex);
      System.exit(1);
    }

    if (kerberosPrincipal == null && kerberosKeyTab != null) {
      System.err.println("Kerberos key tab is specified but not the kerberos principal. Please specify it using the -kp option.");
      System.exit(1);
    }
    if (kerberosPrincipal != null && kerberosKeyTab == null) {
      System.err.println("Kerberos principal is specified but not the kerberos key tab. Please specify it using the -kt option.");
      System.exit(1);
    }

    Level logLevel;
    switch (verboseLevel) {
      case 0:
        logLevel = Level.OFF;
        break;
      case 1:
        logLevel = Level.ERROR;
        break;
      case 2:
        logLevel = Level.WARN;
        break;
      case 3:
        logLevel = Level.INFO;
        break;
      default:
        logLevel = Level.DEBUG;
        break;
    }

    for (org.apache.log4j.Logger logger : new org.apache.log4j.Logger[]{
        org.apache.log4j.Logger.getRootLogger(),
        org.apache.log4j.Logger.getLogger(ApexCli.class)
    }) {

     /*
      * Override logLevel specified by user, the same logLevel would be inherited by all
      * appenders related to logger.
      */
      logger.setLevel(logLevel);

      @SuppressWarnings("unchecked")
      Enumeration<Appender> allAppenders = logger.getAllAppenders();
      while (allAppenders.hasMoreElements()) {
        Appender appender = allAppenders.nextElement();
        if (appender instanceof ConsoleAppender) {
          ((ConsoleAppender)appender).setThreshold(logLevel);
        }
      }
    }

    if (commandsToExecute != null) {
      for (String command : commandsToExecute) {
        LOG.debug("Command to be executed: {}", command);
      }
    }
    if (kerberosPrincipal != null && kerberosKeyTab != null) {
      StramUserLogin.authenticate(kerberosPrincipal, kerberosKeyTab);
    } else {
      Configuration config = ClusterProviderFactory.getProvider().getConfiguration().getNativeConfig();
      StramClientUtils.addDTLocalResources(config);
      StramUserLogin.attemptAuthentication(config);
    }
  }

  /**
   * get highlight color based on env variable first and then config
   *
   */
  protected String getHighlightColor()
  {
    if (highlightColor == null) {
      highlightColor = System.getenv(APEX_HIGHLIGHT_COLOR_ENV_VAR_NAME);
      if (StringUtils.isBlank(highlightColor)) {
        highlightColor = conf.get(APEX_HIGHLIGHT_COLOR_PROPERTY_NAME, FORMAT_BOLD);
      }
      highlightColor = highlightColor.replace("\\e", "\033");
    }
    return highlightColor;
  }

  public void init() throws IOException
  {
    conf = StramClientUtils.addDTSiteResources(ClusterProviderFactory.getProvider().getConfiguration().getNativeConfig());
    SecurityUtils.init(conf);
    fs = StramClientUtils.newFileSystemInstance(conf);
    stramAgent = ClusterProviderFactory.getProvider().getStramAgent(fs, conf);

    makeApexClientContext();
    LOG.debug("Yarn Client initialized and started");
    String socks = conf.get(CommonConfigurationKeysPublic.HADOOP_SOCKS_SERVER_KEY);
    if (socks != null) {
      int colon = socks.indexOf(':');
      if (colon > 0) {
        LOG.info("Using socks proxy at {}", socks);
        System.setProperty("socksProxyHost", socks.substring(0, colon));
        System.setProperty("socksProxyPort", socks.substring(colon + 1));
      }
    }
  }

  protected abstract void makeApexClientContext();

  public abstract boolean isCurrentApp();

  public abstract String currentAppIdToString();

  public abstract String getCurrentAppTrackingUrl();

  public abstract void setApplication(String appId);

  public void processSourceFile(String fileName, ConsoleReader reader) throws FileNotFoundException, IOException
  {
    fileName = expandFileName(fileName, true);
    LOG.debug("Sourcing {}", fileName);
    boolean consolePresentSaved = consolePresent;
    consolePresent = false;
    FileLineReader fr = null;
    String line;
    try {
      fr = new FileLineReader(fileName);
      while ((line = fr.readLine("")) != null) {
        processLine(line, fr, true);
      }
    } finally {
      consolePresent = consolePresentSaved;
      if (fr != null) {
        fr.close();
      }
    }
  }

  private List<Completer> defaultCompleters()
  {
    Map<String, CommandSpec> commands = new TreeMap<>();

    commands.putAll(logicalPlanChangeCommands);
    commands.putAll(connectedCommands);
    commands.putAll(globalCommands);

    List<Completer> completers = new LinkedList<>();
    for (Map.Entry<String, CommandSpec> entry : commands.entrySet()) {
      String command = entry.getKey();
      CommandSpec cs = entry.getValue();
      List<Completer> argCompleters = new LinkedList<>();
      argCompleters.add(new StringsCompleter(command));
      Arg[] args = (Arg[])ArrayUtils.addAll(cs.requiredArgs, cs.optionalArgs);
      if (args != null) {
        if (cs instanceof OptionsCommandSpec) {
          // ugly hack because jline cannot dynamically change completer while user types
          if (args[0] instanceof FileArg || args[0] instanceof VarArg) {
            for (int i = 0; i < 10; i++) {
              argCompleters.add(new MyFileNameCompleter());
            }
          }
        } else {
          for (Arg arg : args) {
            if (arg instanceof FileArg || arg instanceof VarArg) {
              argCompleters.add(new MyFileNameCompleter());
            } else if (arg instanceof CommandArg) {
              argCompleters.add(new StringsCompleter(commands.keySet().toArray(new String[]{})));
            } else {
              argCompleters.add(MyNullCompleter.INSTANCE);
            }
          }
        }
      }

      completers.add(new ArgumentCompleter(argCompleters));
    }

    List<Completer> argCompleters = new LinkedList<>();
    Set<String> set = new TreeSet<>();
    set.addAll(aliases.keySet());
    set.addAll(macros.keySet());
    argCompleters.add(new StringsCompleter(set.toArray(new String[]{})));
    for (int i = 0; i < 10; i++) {
      argCompleters.add(new MyFileNameCompleter());
    }
    completers.add(new ArgumentCompleter(argCompleters));
    return completers;
  }

  private void setupCompleter(ConsoleReader reader)
  {
    reader.addCompleter(new AggregateCompleter(defaultCompleters()));
  }

  public void updateCompleter(ConsoleReader reader)
  {
    List<Completer> completers = new ArrayList<>(reader.getCompleters());
    for (Completer c : completers) {
      reader.removeCompleter(c);
    }
    setupCompleter(reader);
  }

  private void setupHistory(ConsoleReader reader)
  {
    File historyFile = new File(StramClientUtils.getUserDTDirectory(), "cli_history");
    historyFile.getParentFile().mkdirs();
    try {
      topLevelHistory = new FileHistory(historyFile);
      reader.setHistory(topLevelHistory);
      historyFile = new File(StramClientUtils.getUserDTDirectory(), "cli_history_clp");
      changingLogicalPlanHistory = new FileHistory(historyFile);
    } catch (IOException ex) {
      System.err.printf("Unable to open %s for writing.", historyFile);
    }
  }

  private void setupAgents() throws IOException
  {
    recordingsAgent = new RecordingsAgent(stramAgent);
  }

  public void run() throws IOException
  {
    ConsoleReader reader = new ConsoleReader();
    reader.setExpandEvents(false);
    reader.setBellEnabled(false);
    try {
      processSourceFile(StramClientUtils.getConfigDir() + "/clirc_system", reader);
    } catch (Exception ex) {
      // ignore
    }
    try {
      processSourceFile(StramClientUtils.getUserDTDirectory() + "/clirc", reader);
    } catch (Exception ex) {
      // ignore
    }
    if (consolePresent) {
      printWelcomeMessage();
      setupCompleter(reader);
      setupHistory(reader);
      //reader.setHandleUserInterrupt(true);
    } else {
      reader.setEchoCharacter((char)0);
    }
    setupAgents();
    String line;
    PrintWriter out = new PrintWriter(System.out);
    int i = 0;
    while (true) {
      if (commandsToExecute != null) {
        if (i >= commandsToExecute.length) {
          break;
        }
        line = commandsToExecute[i++];
      } else {
        line = readLine(reader);
        if (line == null) {
          break;
        }
      }
      processLine(line, reader, true);
      out.flush();
    }
    if (topLevelHistory != null) {
      try {
        topLevelHistory.flush();
      } catch (IOException ex) {
        LOG.warn("Cannot flush command history", ex);
      }
    }
    if (changingLogicalPlanHistory != null) {
      try {
        changingLogicalPlanHistory.flush();
      } catch (IOException ex) {
        LOG.warn("Cannot flush command history", ex);
      }
    }
    if (consolePresent) {
      System.out.println("exit");
    }
  }

  private List<String> expandMacro(List<String> lines, String[] args)
  {
    List<String> expandedLines = new ArrayList<>();

    for (String line : lines) {
      int previousIndex = 0;
      StringBuilder expandedLine = new StringBuilder(line.length());
      while (true) {
        // Search for $0..$9 within the each line and replace by corresponding args
        int currentIndex = line.indexOf('$', previousIndex);
        if (currentIndex > 0 && line.length() > currentIndex + 1) {
          int argIndex = line.charAt(currentIndex + 1) - '0';
          if (args.length > argIndex && argIndex >= 0) {
            // Replace $0 with macro name or $1..$9 with input arguments
            expandedLine.append(line.substring(previousIndex, currentIndex)).append(args[argIndex]);
          } else if (argIndex >= 0 && argIndex <= 9) {
            // Arguments for $1..$9 were not supplied - replace with empty strings
            expandedLine.append(line.substring(previousIndex, currentIndex));
          } else {
            // Outside valid arguments range - ignore and do not replace
            expandedLine.append(line.substring(previousIndex, currentIndex + 2));
          }
          currentIndex += 2;
        } else {
          expandedLine.append(line.substring(previousIndex));
          expandedLines.add(expandedLine.toString());
          break;
        }
        previousIndex = currentIndex;
      }
    }
    return expandedLines;
  }

  public static String ltrim(String s)
  {
    int i = 0;
    while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
      i++;
    }
    return s.substring(i);
  }

  protected void processLine(String line, final ConsoleReader reader, boolean expandMacroAlias)
  {
    try {
      // clear interrupt flag
      Thread.interrupted();
      if (reader.isHistoryEnabled()) {
        History history = reader.getHistory();
        if (history instanceof FileHistory) {
          try {
            ((FileHistory)history).flush();
          } catch (IOException ex) {
            // ignore
          }
        }
      }
      //LOG.debug("line: \"{}\"", line);
      List<String[]> commands = tokenizer.tokenize(line);
      if (commands == null) {
        return;
      }
      for (final String[] args : commands) {
        if (args.length == 0 || StringUtils.isBlank(args[0])) {
          continue;
        }
        //LOG.debug("Got: {}", mapper.writeValueAsString(args));
        if (expandMacroAlias) {
          if (macros.containsKey(args[0])) {
            List<String> macroItems = expandMacro(macros.get(args[0]), args);
            for (String macroItem : macroItems) {
              if (consolePresent) {
                System.out.println("expanded-macro> " + macroItem);
              }
              processLine(macroItem, reader, false);
            }
            continue;
          }

          if (aliases.containsKey(args[0])) {
            processLine(aliases.get(args[0]), reader, false);
            continue;
          }
        }
        CommandSpec cs = null;
        if (changingLogicalPlan) {
          cs = logicalPlanChangeCommands.get(args[0]);
        } else {
          if (isCurrentApp()) {
            cs = connectedCommands.get(args[0]);
          }
          if (cs == null) {
            cs = globalCommands.get(args[0]);
          }
        }
        if (cs == null) {
          if (connectedCommands.get(args[0]) != null) {
            System.err.println("\"" + args[0] + "\" is valid only when connected to an application. Type \"connect <appid>\" to connect to an application.");
            lastCommandError = true;
          } else if (logicalPlanChangeCommands.get(args[0]) != null) {
            System.err.println("\"" + args[0] + "\" is valid only when changing a logical plan.  Type \"begin-logical-plan-change\" to change a logical plan");
            lastCommandError = true;
          } else {
            System.err.println("Invalid command '" + args[0] + "'. Type \"help\" for list of commands");
            lastCommandError = true;
          }
        } else {
          try {
            cs.verifyArguments(args);
          } catch (CliException ex) {
            cs.printUsage(args[0]);
            throw ex;
          }
          final Command command = cs.command;
          commandThread = new CommandThread(this)
          {
            @Override
            public void run()
            {
              try {
                command.execute(args, reader, apexCli);
                lastCommandError = false;
              } catch (Exception e) {
                handleException(e);
              } catch (Error e) {
                handleException(e);
                System.err.println("Fatal error encountered");
                System.exit(1);
              }
            }

          };
          mainThread = Thread.currentThread();
          commandThread.start();
          try {
            commandThread.join();
          } catch (InterruptedException ex) {
            System.err.println("Interrupted");
          }
          commandThread = null;
        }
      }
    } catch (Exception e) {
      handleException(e);
    }
  }

  private static class CommandThread extends Thread
  {
    final ApexCli apexCli;

    private CommandThread(ApexCli apexCli)
    {
      this.apexCli = apexCli;
    }
  }

  private void handleException(Throwable e)
  {
    System.err.println(ExceptionUtils.getFullStackTrace(e));
    LOG.error("Exception caught: ", e);
    lastCommandError = true;
  }

  private void printWelcomeMessage()
  {
    VersionInfo v = VersionInfo.APEX_VERSION;
    System.out.println("Apex CLI " + v.getVersion() + " " + v.getDate() + " " + v.getRevision());
  }

  public void printHelp(String command, CommandSpec commandSpec, PrintStream os)
  {
    if (consolePresent) {
      os.print(getHighlightColor());
      os.print(command);
      os.print(COLOR_RESET);
    } else {
      os.print(command);
    }
    if (commandSpec instanceof OptionsCommandSpec) {
      OptionsCommandSpec ocs = (OptionsCommandSpec)commandSpec;
      if (ocs.options != null) {
        os.print(" [options]");
      }
    }
    if (commandSpec.requiredArgs != null) {
      for (Arg arg : commandSpec.requiredArgs) {
        if (consolePresent) {
          os.print(" " + ITALICS + arg + COLOR_RESET);
        } else {
          os.print(" <" + arg + ">");
        }
      }
    }
    if (commandSpec.optionalArgs != null) {
      for (Arg arg : commandSpec.optionalArgs) {
        if (consolePresent) {
          os.print(" [" + ITALICS + arg + COLOR_RESET);
        } else {
          os.print(" [<" + arg + ">");
        }
        if (arg instanceof VarArg) {
          os.print(" ...");
        }
        os.print("]");
      }
    }
    os.println("\n\t" + commandSpec.description);
    if (commandSpec instanceof OptionsCommandSpec) {
      OptionsCommandSpec ocs = (OptionsCommandSpec)commandSpec;
      if (ocs.options != null) {
        os.println("\tOptions:");
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(os);
        formatter.printOptions(pw, 80, ocs.options, 12, 4);
        pw.flush();
      }
    }
  }

  public void printHelp(Map<String, CommandSpec> commandSpecs, PrintStream os)
  {
    for (Map.Entry<String, CommandSpec> entry : commandSpecs.entrySet()) {
      printHelp(entry.getKey(), entry.getValue(), os);
    }
  }

  private String readLine(ConsoleReader reader)
    throws IOException
  {
    if (forcePrompt == null) {
      prompt = "";
      if (consolePresent) {
        if (changingLogicalPlan) {
          prompt = "logical-plan-change";
        } else {
          prompt = "apex";
        }
        if (isCurrentApp()) {
          prompt += " (";
          prompt += currentAppIdToString();
          prompt += ") ";
        }
        prompt += "> ";
      }
    } else {
      prompt = forcePrompt;
    }
    String line = reader.readLine(prompt, consolePresent ? null : (char)0);
    if (line == null) {
      return null;
    }
    return ltrim(line);
  }

  public abstract String getContainerLongId(String containerId);

  public List<AppFactory> getMatchingAppFactories(StramAppLauncher submitApp, String matchString, boolean exactMatch)
  {
    try {
      List<AppFactory> cfgList = submitApp.getBundledTopologies();

      if (cfgList.isEmpty()) {
        return null;
      } else if (matchString == null) {
        return cfgList;
      } else {
        List<AppFactory> result = new ArrayList<>();
        if (!exactMatch) {
          matchString = matchString.toLowerCase();
        }
        for (AppFactory ac : cfgList) {
          String appName = ac.getName();
          String appAlias = submitApp.getLogicalPlanConfiguration().getAppAlias(appName);
          if (exactMatch) {
            if (matchString.equals(appName) || matchString.equals(appAlias)) {
              result.add(ac);
            }
          } else if (appName.toLowerCase().contains(matchString) || (appAlias != null && appAlias.toLowerCase()
              .contains(matchString))) {
            result.add(ac);
          }
        }
        return result;
      }
    } catch (Exception ex) {
      LOG.warn("Caught Exception: ", ex);
      return null;
    }
  }

  public File copyToLocal(String[] files) throws IOException
  {
    File tmpDir = new File(System.getProperty("java.io.tmpdir") + "/datatorrent/" + ManagementFactory.getRuntimeMXBean().getName());
    tmpDir.mkdirs();
    for (int i = 0; i < files.length; i++) {
      try {
        URI uri = new URI(files[i]);
        String scheme = uri.getScheme();
        if (scheme == null || scheme.equals("file")) {
          files[i] = uri.getPath();
        } else {
          try (FileSystem tmpFs = FileSystem.newInstance(uri, conf)) {
            Path srcPath = new Path(uri.getPath());
            Path dstPath = new Path(tmpDir.getAbsolutePath(), String.valueOf(i) + srcPath.getName());
            tmpFs.copyToLocalFile(srcPath, dstPath);
            files[i] = dstPath.toUri().getPath();
          }
        }
      } catch (URISyntaxException ex) {
        throw new RuntimeException(ex);
      }
    }

    return tmpDir;
  }

  static {
    GetAppPackageInfoCommandLineInfo.GET_APP_PACKAGE_INFO_OPTIONS
        .addOption(new Option("withDescription", false, "Get default properties with description"));
  }

  public void checkConfigPackageCompatible(AppPackage ap, ConfigPackage cp)
  {
    if (cp == null) {
      return;
    }
    String requiredAppPackageName = cp.getAppPackageName();
    String requiredAppPackageGroupId = cp.getAppPackageGroupId();
    if (requiredAppPackageName != null && !requiredAppPackageName.equals(ap.getAppPackageName())) {
      throw new CliException("Config package requires an app package name of \"" + requiredAppPackageName + "\". The app package given has the name of \"" + ap.getAppPackageName() + "\"");
    }
    if (requiredAppPackageGroupId != null && !requiredAppPackageGroupId.equals(ap.getAppPackageGroupId())) {
      throw new CliException("Config package requires an app package group id of \"" + requiredAppPackageGroupId +
          "\". The app package given has the groupId of \"" + ap.getAppPackageGroupId() + "\"");
    }
    String requiredAppPackageMinVersion = cp.getAppPackageMinVersion();
    if (requiredAppPackageMinVersion != null && VersionInfo.compare(requiredAppPackageMinVersion, ap.getAppPackageVersion()) > 0) {
      throw new CliException("Config package requires an app package minimum version of \"" + requiredAppPackageMinVersion + "\". The app package given is of version \"" + ap.getAppPackageVersion() + "\"");
    }
    String requiredAppPackageMaxVersion = cp.getAppPackageMaxVersion();
    if (requiredAppPackageMaxVersion != null && VersionInfo.compare(requiredAppPackageMaxVersion, ap.getAppPackageVersion()) < 0) {
      throw new CliException("Config package requires an app package maximum version of \"" + requiredAppPackageMaxVersion + "\". The app package given is of version \"" + ap.getAppPackageVersion() + "\"");
    }
  }

  public void checkPlatformCompatible(AppPackage ap)
  {
    String apVersion = ap.getDtEngineVersion();
    VersionInfo actualVersion = VersionInfo.APEX_VERSION;
    if (!VersionInfo.isCompatible(actualVersion.getVersion(), apVersion)) {
      throw new CliException("This App Package is compiled with Apache Apex Core API version " + apVersion + ", which is incompatible with this Apex Core version " + actualVersion.getVersion());
    }
  }

  public void launchAppPackage(AppPackage ap, ConfigPackage cp, LaunchCommandLineInfo commandLineInfo, ConsoleReader reader) throws Exception
  {
    getLaunchCommand().execute(getLaunchAppPackageArgs(ap, cp, commandLineInfo, reader), reader, this);
  }

  String[] getLaunchAppPackageArgs(AppPackage ap, ConfigPackage cp, LaunchCommandLineInfo commandLineInfo, ConsoleReader reader) throws Exception
  {
    String matchAppName = null;
    if (commandLineInfo.args.length > 1) {
      matchAppName = commandLineInfo.args[1];
    }

    List<AppInfo> applications = new ArrayList<>(getAppsFromPackageAndConfig(ap, cp, commandLineInfo.useConfigApps));

    if (matchAppName != null) {
      Iterator<AppInfo> it = applications.iterator();
      while (it.hasNext()) {
        AppInfo ai = it.next();
        if ((commandLineInfo.exactMatch && !ai.name.equals(matchAppName))
            || !ai.name.toLowerCase().matches(".*" + matchAppName.toLowerCase() + ".*")) {
          it.remove();
        }
      }
    }

    AppInfo selectedApp = null;

    if (applications.isEmpty()) {
      throw new CliException("No applications in Application Package" + (matchAppName != null ? " matching \"" + matchAppName + "\"" : ""));
    } else if (applications.size() == 1) {
      selectedApp = applications.get(0);
    } else {
      //Store the appNames sorted in alphabetical order and their position in matchingAppFactories list
      TreeMap<String, Integer> appNamesInAlphabeticalOrder = new TreeMap<>();
      // Display matching applications
      for (int i = 0; i < applications.size(); i++) {
        String appName = applications.get(i).name;
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
      if (!consolePresent) {
        throw new CliException("More than one application in Application Package match '" + matchAppName + "'");
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
          if (0 < option && option <= applications.size()) {
            int appIndex = displayIndexToOriginalUnsortedIndexMap.get(option);
            selectedApp = applications.get(appIndex);
          }
        } catch (Exception ex) {
          // ignore
        }
      }
    }

    if (selectedApp == null) {
      throw new CliException("No application selected");
    }

    DTConfiguration launchProperties = getLaunchAppPackageProperties(ap, cp, commandLineInfo, selectedApp.name);
    String appFile = ap.tempDirectory() + "/app/" + selectedApp.file;

    List<String> launchArgs = new ArrayList<>();

    launchArgs.add("launch");
    launchArgs.add("-exactMatch");
    List<String> absClassPath = new ArrayList<>(ap.getClassPath());
    for (int i = 0; i < absClassPath.size(); i++) {
      String path = absClassPath.get(i);
      if (!path.startsWith("/")) {
        absClassPath.set(i, ap.tempDirectory() + "/" + path);
      }
    }

    if (cp != null) {
      StringBuilder files = new StringBuilder();
      for (String file : cp.getClassPath()) {
        if (files.length() != 0) {
          files.append(',');
        }
        files.append(cp.tempDirectory()).append(File.separatorChar).append(file);
      }
      if (!StringUtils.isBlank(files.toString())) {
        if (commandLineInfo.libjars != null) {
          commandLineInfo.libjars = files.toString() + "," + commandLineInfo.libjars;
        } else {
          commandLineInfo.libjars = files.toString();
        }
      }

      files.setLength(0);
      for (String file : cp.getFiles()) {
        if (files.length() != 0) {
          files.append(',');
        }
        files.append(cp.tempDirectory()).append(File.separatorChar).append(file);
      }
      if (!StringUtils.isBlank(files.toString())) {
        if (commandLineInfo.files != null) {
          commandLineInfo.files = files.toString() + "," + commandLineInfo.files;
        } else {
          commandLineInfo.files = files.toString();
        }
      }
    }

    StringBuilder libjarsVal = new StringBuilder();
    if (!absClassPath.isEmpty() || commandLineInfo.libjars != null) {
      if (!absClassPath.isEmpty()) {
        libjarsVal.append(org.apache.commons.lang3.StringUtils.join(absClassPath, ','));
      }
      if (commandLineInfo.libjars != null) {
        if (libjarsVal.length() > 0) {
          libjarsVal.append(",");
        }
        libjarsVal.append(commandLineInfo.libjars);
      }
    }
    if (appFile.endsWith(".json") || appFile.endsWith(".properties")) {
      if (libjarsVal.length() > 0) {
        libjarsVal.append(",");
      }
      libjarsVal.append(ap.tempDirectory()).append("/app/*.jar");
    }
    if (libjarsVal.length() > 0) {
      launchArgs.add("-libjars");
      launchArgs.add(libjarsVal.toString());
    }

    File launchPropertiesFile = new File(ap.tempDirectory(), "launch.xml");
    launchProperties.writeToFile(launchPropertiesFile, "");
    launchArgs.add("-conf");
    launchArgs.add(launchPropertiesFile.getCanonicalPath());
    if (commandLineInfo.localMode) {
      launchArgs.add("-local");
    }
    if (commandLineInfo.archives != null) {
      launchArgs.add("-archives");
      launchArgs.add(commandLineInfo.archives);
    }
    if (commandLineInfo.files != null) {
      launchArgs.add("-files");
      launchArgs.add(commandLineInfo.files);
    }
    if (commandLineInfo.origAppId != null) {
      launchArgs.add("-originalAppId");
      launchArgs.add(commandLineInfo.origAppId);
    }
    if (commandLineInfo.queue != null) {
      launchArgs.add("-queue");
      launchArgs.add(commandLineInfo.queue);
    }
    if (commandLineInfo.tags != null) {
      launchArgs.add("-tags");
      launchArgs.add(commandLineInfo.tags);
    }
    launchArgs.add(appFile);
    if (!appFile.endsWith(".json") && !appFile.endsWith(".properties")) {
      launchArgs.add(selectedApp.name);
    }

    LOG.debug("Launch command: {}", StringUtils.join(launchArgs, " "));
    return launchArgs.toArray(new String[]{});
  }

  DTConfiguration getLaunchAppPackageProperties(AppPackage ap, ConfigPackage cp, LaunchCommandLineInfo commandLineInfo, String appName) throws Exception
  {
    DTConfiguration launchProperties = new DTConfiguration();

    List<AppInfo> applications = getAppsFromPackageAndConfig(ap, cp, commandLineInfo.useConfigApps);

    AppInfo selectedApp = null;
    for (AppInfo app : applications) {
      if (app.name.equals(appName)) {
        selectedApp = app;
        break;
      }
    }
    Map<String, PropertyInfo> defaultProperties = selectedApp == null ? ap.getDefaultProperties() : selectedApp.defaultProperties;
    Set<String> requiredProperties = new TreeSet<>(selectedApp == null ? ap.getRequiredProperties() : selectedApp.requiredProperties);

    for (Map.Entry<String, PropertyInfo> entry : defaultProperties.entrySet()) {
      launchProperties.set(entry.getKey(), entry.getValue().getValue(), Scope.TRANSIENT, entry.getValue().getDescription());
      requiredProperties.remove(entry.getKey());
    }

      // settings specified in the user's environment take precedence over defaults in package.
    // since both are merged into a single -conf option below, apply them on top of the defaults here.
    File confFile = new File(StramClientUtils.getUserDTDirectory(), StramClientUtils.DT_SITE_XML_FILE);
    if (confFile.exists()) {
      Configuration userConf = new Configuration(false);
      userConf.addResource(new Path(confFile.toURI()));
      Iterator<Entry<String, String>> it = userConf.iterator();
      while (it.hasNext()) {
        Entry<String, String> entry = it.next();
        // filter relevant entries
        String key = entry.getKey();
        if (key.startsWith(StreamingApplication.DT_PREFIX)
            || key.startsWith(StreamingApplication.APEX_PREFIX)) {
          launchProperties.set(key, entry.getValue(), Scope.TRANSIENT, null);
          requiredProperties.remove(key);
        }
      }
    }

    if (commandLineInfo.apConfigFile != null) {
      DTConfiguration givenConfig = new DTConfiguration();
      givenConfig.loadFile(new File(ap.tempDirectory() + "/conf/" + commandLineInfo.apConfigFile));
      for (Map.Entry<String, String> entry : givenConfig) {
        launchProperties.set(entry.getKey(), entry.getValue(), Scope.TRANSIENT, null);
        requiredProperties.remove(entry.getKey());
      }
    }
    if (cp != null) {
      Map<String, String> properties = cp.getProperties(appName);
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        launchProperties.set(entry.getKey(), entry.getValue(), Scope.TRANSIENT, null);
        requiredProperties.remove(entry.getKey());
      }
    } else if (commandLineInfo.configFile != null) {
      DTConfiguration givenConfig = new DTConfiguration();
      givenConfig.loadFile(new File(commandLineInfo.configFile));
      for (Map.Entry<String, String> entry : givenConfig) {
        launchProperties.set(entry.getKey(), entry.getValue(), Scope.TRANSIENT, null);
        requiredProperties.remove(entry.getKey());
      }
    }
    if (commandLineInfo.overrideProperties != null) {
      for (Map.Entry<String, String> entry : commandLineInfo.overrideProperties.entrySet()) {
        launchProperties.set(entry.getKey(), entry.getValue(), Scope.TRANSIENT, null);
        requiredProperties.remove(entry.getKey());
      }
    }

    // now look at whether it is in default configuration
    for (Map.Entry<String, String> entry : conf) {
      if (StringUtils.isNotBlank(entry.getValue())) {
        requiredProperties.remove(entry.getKey());
      }
    }
    if (!requiredProperties.isEmpty()) {
      throw new CliException("Required properties not set: " + StringUtils.join(requiredProperties, ", "));
    }

    //StramClientUtils.evalProperties(launchProperties);
    return launchProperties;
  }

  private List<AppInfo> getAppsFromPackageAndConfig(AppPackage ap, ConfigPackage cp, String configApps)
  {
    if (cp == null || configApps == null || !(configApps.equals(CONFIG_INCLUSIVE) || configApps.equals(CONFIG_EXCLUSIVE))) {
      return ap.getApplications();
    }

    File src = new File(cp.tempDirectory(), "app");
    File dest = new File(ap.tempDirectory(), "app");

    if (!src.exists()) {
      return ap.getApplications();
    }

    if (configApps.equals(CONFIG_EXCLUSIVE)) {

      for (File file : dest.listFiles()) {

        if (file.getName().endsWith(".json")) {
          FileUtils.deleteQuietly(new File(dest, file.getName()));
        }
      }
    } else {
      for (File file : src.listFiles()) {
        FileUtils.deleteQuietly(new File(dest, file.getName()));
      }
    }

    for (File file : src.listFiles()) {
      try {
        FileUtils.moveFileToDirectory(file, dest, true);
      } catch (IOException e) {
        LOG.warn("Application from the config file {} failed while processing.", file.getName());
      }
    }

    try {
      FileUtils.deleteDirectory(src);
    } catch (IOException e) {
      LOG.warn("Failed to delete the Config Apps folder");
    }

    ap.processAppDirectory(configApps.equals(CONFIG_EXCLUSIVE));

    return ap.getApplications();
  }

  public enum AttributesType
  {
    APPLICATION, OPERATOR, PORT
  }

  public void mainHelper() throws Exception
  {
    init();
    run();
    System.exit(lastCommandError ? 1 : 0);
  }

  public static void main(final String[] args) throws Exception
  {
    LoggerUtil.setupMDC("client");
    final ApexCli shell = ClusterProviderFactory.getProvider().getApexCli();
    shell.preImpersonationInit(args);
    String hadoopUserName = System.getenv("HADOOP_USER_NAME");
    if (UserGroupInformation.isSecurityEnabled()
        && StringUtils.isNotBlank(hadoopUserName)
        && !hadoopUserName.equals(UserGroupInformation.getLoginUser().getUserName())) {
      LOG.info("You ({}) are running as user {}", UserGroupInformation.getLoginUser().getUserName(), hadoopUserName);
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(hadoopUserName, UserGroupInformation.getLoginUser());
      ugi.doAs(new PrivilegedExceptionAction<Void>()
      {
        @Override
        public Void run() throws Exception
        {
          shell.mainHelper();
          return null;
        }
      });
    } else {
      shell.mainHelper();
    }
  }
}
