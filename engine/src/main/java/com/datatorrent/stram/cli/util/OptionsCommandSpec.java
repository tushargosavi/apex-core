package com.datatorrent.stram.cli.util;

import java.io.PrintWriter;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.datatorrent.stram.cli.commands.Command;

public class OptionsCommandSpec extends CommandSpec
{
  public Options options;

  public OptionsCommandSpec(Command command, Arg[] requiredArgs, Arg[] optionalArgs, String description, Options options)
  {
    super(command, requiredArgs, optionalArgs, description);
    this.options = options;
  }

  @Override
  public void verifyArguments(String[] args) throws CliException
  {
    try {
      args = new PosixParser().parse(options, args).getArgs();
      super.verifyArguments(args);
    } catch (Exception ex) {
      throw new CliException("Command parameter error");
    }
  }

  @Override
  public void printUsage(String cmd)
  {
    super.printUsage(cmd + ((options == null) ? "" : " [options]"));
    if (options != null) {
      System.out.println("Options:");
      HelpFormatter formatter = new HelpFormatter();
      PrintWriter pw = new PrintWriter(System.out);
      formatter.printOptions(pw, 80, options, 4, 4);
      pw.flush();
    }
  }
}
