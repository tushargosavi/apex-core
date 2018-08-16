package com.datatorrent.stram.cli.util;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

public class GetPhysicalPropertiesCommandLineOptions
{
  public static GetPhysicalPropertiesCommandLineOptions GET_PHYSICAL_PROPERTY_OPTIONS = new GetPhysicalPropertiesCommandLineOptions();

  public final Options options = new Options();
  public final Option propertyName = add(OptionBuilder.withArgName("property name").hasArg().withDescription("The name of the property whose value needs to be retrieved").create("propertyName"));
  public final Option waitTime = add(OptionBuilder.withArgName("wait time").hasArg().withDescription("How long to wait to get the result").create("waitTime"));

  private Option add(Option opt)
  {
    this.options.addOption(opt);
    return opt;
  }
}
