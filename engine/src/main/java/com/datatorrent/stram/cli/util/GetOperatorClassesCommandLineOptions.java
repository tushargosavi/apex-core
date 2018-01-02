package com.datatorrent.stram.cli.util;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class GetOperatorClassesCommandLineOptions
{
  public final Options options = new Options();

  public GetOperatorClassesCommandLineOptions()
  {
    add(new Option("parent", true, "Specify the parent class for the operators"));
  }

  private Option add(Option opt)
  {
    this.options.addOption(opt);
    return opt;
  }
}
