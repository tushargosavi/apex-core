package com.datatorrent.stram.cli.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class GetOperatorClassesCommandLineInfo
{
  public static GetOperatorClassesCommandLineOptions GET_OPERATOR_CLASSES_OPTIONS = new GetOperatorClassesCommandLineOptions();

  public String parent;
  public String[] args;

  public static GetOperatorClassesCommandLineInfo getGetOperatorClassesCommandLineInfo(String[] args) throws ParseException
  {
    CommandLineParser parser = new PosixParser();
    GetOperatorClassesCommandLineInfo result = new GetOperatorClassesCommandLineInfo();
    CommandLine line = parser.parse(GET_OPERATOR_CLASSES_OPTIONS.options, args);
    result.parent = line.getOptionValue("parent");
    result.args = line.getArgs();
    return result;
  }
}
