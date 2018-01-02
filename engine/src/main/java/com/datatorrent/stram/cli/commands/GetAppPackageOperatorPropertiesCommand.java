package com.datatorrent.stram.cli.commands;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.client.AppPackage;

import jline.console.ConsoleReader;

public class GetAppPackageOperatorPropertiesCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    try (AppPackage ap = apexCli.newAppPackageInstance(new URI(args[1]), true)) {
      List<String> newArgs = new ArrayList<>();
      List<String> jars = new ArrayList<>();
      for (String jar : ap.getAppJars()) {
        jars.add(ap.tempDirectory() + "/app/" + jar);
      }
      for (String libJar : ap.getClassPath()) {
        jars.add(ap.tempDirectory() + "/" + libJar);
      }
      newArgs.add("get-jar-operator-properties");
      newArgs.add(StringUtils.join(jars, ","));
      newArgs.add(args[2]);
      new GetJarOperatorPropertiesCommand().execute(newArgs.toArray(new String[]{}), reader, apexCli);
    }
  }
}
