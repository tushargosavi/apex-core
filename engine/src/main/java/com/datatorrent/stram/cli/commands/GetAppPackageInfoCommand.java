package com.datatorrent.stram.cli.commands;

import java.net.URI;

import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.GetAppPackageInfoCommandLineInfo;
import com.datatorrent.stram.client.AppPackage;
import com.datatorrent.stram.util.JSONSerializationProvider;

import jline.console.ConsoleReader;

public class GetAppPackageInfoCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String[] tmpArgs = new String[args.length - 2];
    System.arraycopy(args, 2, tmpArgs, 0, args.length - 2);
    GetAppPackageInfoCommandLineInfo commandLineInfo = GetAppPackageInfoCommandLineInfo.getGetAppPackageInfoCommandLineInfo(tmpArgs);
    try (AppPackage ap = apexCli.newAppPackageInstance(new URI(args[1]), true)) {
      JSONSerializationProvider jomp = new JSONSerializationProvider();
      jomp.addSerializer(AppPackage.PropertyInfo.class,
          new AppPackage.PropertyInfoSerializer(commandLineInfo.provideDescription));
      JSONObject apInfo = new JSONObject(jomp.getContext(null).writeValueAsString(ap));
      apInfo.remove("name");
      apexCli.printJson(apInfo);
    }
  }
}
