package com.datatorrent.stram.cli.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class ShowLogicalPlanCommandLineInfo
{
  public String libjars;
  public boolean ignorePom;
  public String[] args;
  public boolean exactMatch;

  public static ShowLogicalPlanCommandLineInfo getShowLogicalPlanCommandLineInfo(String[] args) throws ParseException
  {
    CommandLineParser parser = new PosixParser();
    ShowLogicalPlanCommandLineInfo result = new ShowLogicalPlanCommandLineInfo();
    CommandLine line = parser.parse(getShowLogicalPlanCommandLineOptions(), args);
    result.libjars = line.getOptionValue("libjars");
    result.ignorePom = line.hasOption("ignorepom");
    result.args = line.getArgs();
    result.exactMatch = line.hasOption("exactMatch");
    return result;
  }

  public static Options getShowLogicalPlanCommandLineOptions()
  {
    Options options = new Options();
    Option libjars = OptionBuilder.withArgName("comma separated list of jars").hasArg().withDescription("Specify comma separated jar/resource files to include in the classpath.").create("libjars");
    Option ignorePom = new Option("ignorepom", "Do not run maven to find the dependency");
    Option exactMatch = new Option("exactMatch", "Only consider exact match for app name");
    options.addOption(libjars);
    options.addOption(ignorePom);
    options.addOption(exactMatch);
    return options;
  }
}

