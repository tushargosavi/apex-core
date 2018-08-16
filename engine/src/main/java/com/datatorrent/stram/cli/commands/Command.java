package com.datatorrent.stram.cli.commands;

import com.datatorrent.stram.cli.ApexCli;

import jline.console.ConsoleReader;

public interface Command
{
  void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception;
}
