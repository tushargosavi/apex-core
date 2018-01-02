package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;

import jline.console.ConsoleReader;

public class EchoCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    for (int i = 1; i < args.length; i++) {
      if (i > 1) {
        System.out.print(" ");
      }
      System.out.print(args[i]);
    }
    System.out.println();
  }
}
