package com.datatorrent.stram.cli.util;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

public class LaunchCommandLineOptions
{
  public final Options options = new Options();
  final Option local = add(new Option("local", "Run application in local mode."));
  final Option configFile = add(OptionBuilder.withArgName("configuration file").hasArg().withDescription("Specify an application configuration file.").create("conf"));
  final Option apConfigFile = add(OptionBuilder.withArgName("app package configuration file").hasArg().withDescription("Specify an application configuration file within the app package if launching an app package.").create("apconf"));
  final Option defProperty = add(OptionBuilder.withArgName("property=value").hasArg().withDescription("Use value for given property.").create("D"));
  final Option libjars = add(OptionBuilder.withArgName("comma separated list of libjars").hasArg().withDescription("Specify comma separated jar files or other resource files to include in the classpath.").create("libjars"));
  final Option files = add(OptionBuilder.withArgName("comma separated list of files").hasArg().withDescription("Specify comma separated files to be copied on the compute machines.").create("files"));
  final Option archives = add(OptionBuilder.withArgName("comma separated list of archives").hasArg().withDescription("Specify comma separated archives to be unarchived on the compute machines.").create("archives"));
  final Option ignorePom = add(new Option("ignorepom", "Do not run maven to find the dependency"));
  final Option originalAppID = add(OptionBuilder.withArgName("application id").hasArg().withDescription("Specify original application identifier for restart.").create("originalAppId"));
  final Option exactMatch = add(new Option("exactMatch", "Only consider applications with exact app name"));
  final Option queue = add(OptionBuilder.withArgName("queue name").hasArg().withDescription("Specify the queue to launch the application").create("queue"));
  final Option tags = add(OptionBuilder.withArgName("comma separated tags").hasArg().withDescription("Specify the tags for the application").create("tags"));
  final Option force = add(new Option("force", "Force launch the application. Do not check for compatibility"));
  final Option useConfigApps = add(OptionBuilder.withArgName("inclusive or exclusive").hasArg().withDescription("\"inclusive\" - merge the apps in config and app package. \"exclusive\" - only show config package apps.").create("useConfigApps"));

  private Option add(Option opt)
  {
    this.options.addOption(opt);
    return opt;
  }
}
