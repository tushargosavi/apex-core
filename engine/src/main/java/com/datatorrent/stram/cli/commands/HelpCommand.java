package com.datatorrent.stram.cli.commands;

import java.io.PrintStream;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CommandSpec;

import jline.console.ConsoleReader;

public class HelpCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    PrintStream os = apexCli.getOutputPrintStream();
    if (args.length < 2) {
      os.println("GLOBAL COMMANDS EXCEPT WHEN CHANGING LOGICAL PLAN:\n");
      apexCli.printHelp(apexCli.getGlobalCommands(), os);
      os.println();
      os.println("COMMANDS WHEN CONNECTED TO AN APP (via connect <appid>) EXCEPT WHEN CHANGING LOGICAL PLAN:\n");
      apexCli.printHelp(apexCli.getConnectedCommands(), os);
      os.println();
      os.println("COMMANDS WHEN CHANGING LOGICAL PLAN (via begin-logical-plan-change):\n");
      apexCli.printHelp(apexCli.getLogicalPlanChangeCommands(), os);
      os.println();
    } else {
      if (args[1].equals("help")) {
        apexCli.printHelp("help", apexCli.getGlobalCommands().get("help"), os);
      } else {
        boolean valid = false;
        CommandSpec cs = apexCli.getGlobalCommands().get(args[1]);
        if (cs != null) {
          os.println("This usage is valid except when changing logical plan");
          apexCli.printHelp(args[1], cs, os);
          os.println();
          valid = true;
        }
        cs = apexCli.getConnectedCommands().get(args[1]);
        if (cs != null) {
          os.println("This usage is valid when connected to an app except when changing logical plan");
          apexCli.printHelp(args[1], cs, os);
          os.println();
          valid = true;
        }
        cs = apexCli.getLogicalPlanChangeCommands().get(args[1]);
        if (cs != null) {
          os.println("This usage is only valid when changing logical plan (via begin-logical-plan-change)");
          apexCli.printHelp(args[1], cs, os);
          os.println();
          valid = true;
        }
        if (!valid) {
          os.println("Help for \"" + args[1] + "\" does not exist.");
        }
      }
    }
    apexCli.closeOutputPrintStream(os);
  }
}
