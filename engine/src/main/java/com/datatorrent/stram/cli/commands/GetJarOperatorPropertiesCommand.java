package com.datatorrent.stram.cli.commands;

import java.io.File;

import org.apache.commons.io.FileUtils;

import com.datatorrent.api.Operator;
import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.webapp.OperatorDiscoverer;

import jline.console.ConsoleReader;

public class GetJarOperatorPropertiesCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String files = ApexCli.expandCommaSeparatedFiles(args[1]);
    if (files == null) {
      throw new CliException("File " + args[1] + " is not found");
    }
    String[] jarFiles = files.split(",");
    File tmpDir = apexCli.copyToLocal(jarFiles);
    try {
      OperatorDiscoverer operatorDiscoverer = new OperatorDiscoverer(jarFiles);
      Class<? extends Operator> operatorClass = operatorDiscoverer.getOperatorClass(args[2]);
      apexCli.printJson(operatorDiscoverer.describeOperator(operatorClass.getName()));
    } finally {
      FileUtils.deleteDirectory(tmpDir);
    }
  }
}
