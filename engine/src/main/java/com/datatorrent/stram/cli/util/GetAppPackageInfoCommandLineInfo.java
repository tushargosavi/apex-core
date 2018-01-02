package com.datatorrent.stram.cli.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class GetAppPackageInfoCommandLineInfo
{
  public static Options GET_APP_PACKAGE_INFO_OPTIONS = new Options();

  public boolean provideDescription;

  public static GetAppPackageInfoCommandLineInfo getGetAppPackageInfoCommandLineInfo(String[] args) throws ParseException
  {
    CommandLineParser parser = new PosixParser();
    GetAppPackageInfoCommandLineInfo result = new GetAppPackageInfoCommandLineInfo();
    CommandLine line = parser.parse(GET_APP_PACKAGE_INFO_OPTIONS, args);
    result.provideDescription = line.hasOption("withDescription");
    return result;
  }
}
