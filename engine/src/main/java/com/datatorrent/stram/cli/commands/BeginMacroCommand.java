package com.datatorrent.stram.cli.commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.datatorrent.stram.cli.ApexCli;

import jline.console.ConsoleReader;

public class BeginMacroCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String name = args[1];
    if (apexCli.getMacros().containsKey(name) || apexCli.getAliases().containsKey(name)) {
      System.err.println("Name '" + name + "' already exists.");
      return;
    }
    try {
      List<String> commands = new ArrayList<>();
      while (true) {
        String line;
        if (apexCli.isConsolePresent()) {
          line = reader.readLine("macro def (" + name + ") > ");
        } else {
          line = reader.readLine("", (char)0);
        }
        if (line.equals("end")) {
          apexCli.getMacros().put(name, commands);
          apexCli.updateCompleter(reader);
          if (apexCli.isConsolePresent()) {
            System.out.println("Macro '" + name + "' created.");
          }
          return;
        } else if (line.equals("abort")) {
          System.err.println("Aborted");
          return;
        } else {
          commands.add(line);
        }
      }
    } catch (IOException ex) {
      System.err.println("Aborted");
    }
  }
}
