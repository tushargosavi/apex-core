package com.datatorrent.stram.cli.commands;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.GetOperatorClassesCommandLineInfo;
import com.datatorrent.stram.client.AppPackage;

import jline.console.ConsoleReader;

public class GetAppPackageOperatorsCommand implements Command
{
  private static final Logger LOG = LoggerFactory.getLogger(GetAppPackageOperatorsCommand.class);

  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String[] tmpArgs = new String[args.length - 1];
    System.arraycopy(args, 1, tmpArgs, 0, args.length - 1);
    GetOperatorClassesCommandLineInfo commandLineInfo = GetOperatorClassesCommandLineInfo.getGetOperatorClassesCommandLineInfo(tmpArgs);
    try (AppPackage ap = apexCli.newAppPackageInstance(new URI(commandLineInfo.args[0]), true)) {
      List<String> newArgs = new ArrayList<>();
      List<String> jars = new ArrayList<>();
      for (String jar : ap.getAppJars()) {
        jars.add(ap.tempDirectory() + "/app/" + jar);
      }
      for (String libJar : ap.getClassPath()) {
        jars.add(ap.tempDirectory() + "/" + libJar);
      }
      newArgs.add("get-jar-operator-classes");
      if (commandLineInfo.parent != null) {
        newArgs.add("-parent");
        newArgs.add(commandLineInfo.parent);
      }
      newArgs.add(StringUtils.join(jars, ","));
      for (int i = 1; i < commandLineInfo.args.length; i++) {
        newArgs.add(commandLineInfo.args[i]);
      }
      LOG.debug("Executing: " + newArgs);
      new GetJarOperatorClassesCommand().execute(newArgs.toArray(new String[]{}), reader, apexCli);
    }
  }
}
