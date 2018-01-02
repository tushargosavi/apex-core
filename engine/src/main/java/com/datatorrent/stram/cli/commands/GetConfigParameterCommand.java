package com.datatorrent.stram.cli.commands;

import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;

import com.datatorrent.stram.cli.ApexCli;

import jline.console.ConsoleReader;

public class GetConfigParameterCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    PrintStream os = apexCli.getOutputPrintStream();
    if (args.length == 1) {
      Map<String, String> sortedMap = new TreeMap<>();
      for (Map.Entry<String, String> entry : apexCli.getConf()) {
        sortedMap.put(entry.getKey(), entry.getValue());
      }
      for (Map.Entry<String, String> entry : sortedMap.entrySet()) {
        os.println(entry.getKey() + "=" + entry.getValue());
      }
    } else {
      String value = apexCli.getConf().get(args[1]);
      if (value != null) {
        os.println(value);
      }
    }
    apexCli.closeOutputPrintStream(os);
  }
}
