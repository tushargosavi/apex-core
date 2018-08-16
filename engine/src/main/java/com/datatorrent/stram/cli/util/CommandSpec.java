package com.datatorrent.stram.cli.util;

import com.datatorrent.stram.cli.commands.Command;

public class CommandSpec
{
  public Command command;
  public Arg[] requiredArgs;
  public Arg[] optionalArgs;
  public String description;

  public CommandSpec(Command command, Arg[] requiredArgs, Arg[] optionalArgs, String description)
  {
    this.command = command;
    this.requiredArgs = requiredArgs;
    this.optionalArgs = optionalArgs;
    this.description = description;
  }

  public void verifyArguments(String[] args) throws CliException
  {
    int minArgs = 0;
    int maxArgs = 0;
    if (requiredArgs != null) {
      minArgs = requiredArgs.length;
      maxArgs = requiredArgs.length;
    }
    if (optionalArgs != null) {
      for (Arg arg : optionalArgs) {
        if (arg instanceof VarArg) {
          maxArgs = Integer.MAX_VALUE;
          break;
        } else {
          maxArgs++;
        }
      }
    }
    if (args.length - 1 < minArgs || args.length - 1 > maxArgs) {
      throw new CliException("Command parameter error");
    }
  }

  public void printUsage(String cmd)
  {
    System.err.print("Usage: " + cmd);
    if (requiredArgs != null) {
      for (Arg arg : requiredArgs) {
        System.err.print(" <" + arg + ">");
      }
    }
    if (optionalArgs != null) {
      for (Arg arg : optionalArgs) {
        if (arg instanceof VarArg) {
          System.err.print(" [<" + arg + "> ... ]");
        } else {
          System.err.print(" [<" + arg + ">]");
        }
      }
    }
    System.err.println();
  }
}
