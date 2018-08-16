package com.datatorrent.stram.cli.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class LaunchCommandLineInfo
{
  public static LaunchCommandLineOptions LAUNCH_OPTIONS = new LaunchCommandLineOptions();

  public boolean localMode;
  public boolean ignorePom;
  public String configFile;
  public String apConfigFile;
  public Map<String, String> overrideProperties;
  public String libjars;
  public String files;
  public String queue;
  public String tags;
  public String archives;
  public String origAppId;
  public boolean exactMatch;
  public boolean force;
  public String[] args;
  public String useConfigApps;

  public static LaunchCommandLineInfo getLaunchCommandLineInfo(String[] args) throws ParseException
  {
    CommandLineParser parser = new PosixParser();
    LaunchCommandLineInfo result = new LaunchCommandLineInfo();
    CommandLine line = parser.parse(LAUNCH_OPTIONS.options, args);
    result.localMode = line.hasOption(LAUNCH_OPTIONS.local.getOpt());
    result.configFile = line.getOptionValue(LAUNCH_OPTIONS.configFile.getOpt());
    result.apConfigFile = line.getOptionValue(LAUNCH_OPTIONS.apConfigFile.getOpt());
    result.ignorePom = line.hasOption(LAUNCH_OPTIONS.ignorePom.getOpt());
    String[] defs = line.getOptionValues(LAUNCH_OPTIONS.defProperty.getOpt());
    if (defs != null) {
      result.overrideProperties = new HashMap<>();
      for (String def : defs) {
        int equal = def.indexOf('=');
        if (equal < 0) {
          result.overrideProperties.put(def, null);
        } else {
          result.overrideProperties.put(def.substring(0, equal), def.substring(equal + 1));
        }
      }
    }
    result.libjars = line.getOptionValue(LAUNCH_OPTIONS.libjars.getOpt());
    result.archives = line.getOptionValue(LAUNCH_OPTIONS.archives.getOpt());
    result.files = line.getOptionValue(LAUNCH_OPTIONS.files.getOpt());
    result.queue = line.getOptionValue(LAUNCH_OPTIONS.queue.getOpt());
    result.tags = line.getOptionValue(LAUNCH_OPTIONS.tags.getOpt());
    result.args = line.getArgs();
    result.origAppId = line.getOptionValue(LAUNCH_OPTIONS.originalAppID.getOpt());
    result.exactMatch = line.hasOption("exactMatch");
    result.force = line.hasOption("force");
    result.useConfigApps = line.getOptionValue(LAUNCH_OPTIONS.useConfigApps.getOpt());

    return result;
  }
}
