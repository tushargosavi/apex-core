package com.datatorrent.stram.cli.commands;

import java.io.File;
import java.util.Iterator;
import java.util.Set;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import org.apache.commons.io.FileUtils;

import com.datatorrent.api.DAG;
import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.cli.util.CliException;
import com.datatorrent.stram.cli.util.GetOperatorClassesCommandLineInfo;
import com.datatorrent.stram.webapp.OperatorDiscoverer;

import jline.console.ConsoleReader;

public class GetJarOperatorClassesCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    String[] newArgs = new String[args.length - 1];
    System.arraycopy(args, 1, newArgs, 0, args.length - 1);
    GetOperatorClassesCommandLineInfo commandLineInfo = GetOperatorClassesCommandLineInfo.getGetOperatorClassesCommandLineInfo(newArgs);
    String parentName = commandLineInfo.parent != null ? commandLineInfo.parent : DAG.GenericOperator.class.getName();
    String files = ApexCli.expandCommaSeparatedFiles(commandLineInfo.args[0]);
    if (files == null) {
      throw new CliException("File " + commandLineInfo.args[0] + " is not found");
    }
    String[] jarFiles = files.split(",");
    File tmpDir = apexCli.copyToLocal(jarFiles);
    try {
      OperatorDiscoverer operatorDiscoverer = new OperatorDiscoverer(jarFiles);
      String searchTerm = commandLineInfo.args.length > 1 ? commandLineInfo.args[1] : null;
      Set<String> operatorClasses = operatorDiscoverer.getOperatorClasses(parentName, searchTerm);
      JSONObject json = new JSONObject();
      JSONArray arr = new JSONArray();
      JSONObject portClassHier = new JSONObject();
      JSONObject portTypesWithSchemaClasses = new JSONObject();

      JSONObject failed = new JSONObject();

      for (final String clazz : operatorClasses) {
        try {
          JSONObject oper = operatorDiscoverer.describeOperator(clazz);

          // add default value
          operatorDiscoverer.addDefaultValue(clazz, oper);

          // add class hierarchy info to portClassHier and fetch port types with schema classes
          operatorDiscoverer.buildAdditionalPortInfo(oper, portClassHier, portTypesWithSchemaClasses);

          Iterator portTypesIter = portTypesWithSchemaClasses.keys();
          while (portTypesIter.hasNext()) {
            if (!portTypesWithSchemaClasses.getBoolean((String)portTypesIter.next())) {
              portTypesIter.remove();
            }
          }

          arr.put(oper);
        } catch (Exception | NoClassDefFoundError ex) {
          // ignore this class
          final String cls = clazz;
          failed.put(cls, ex.toString());
        }
      }

      json.put("operatorClasses", arr);
      json.put("portClassHier", portClassHier);
      json.put("portTypesWithSchemaClasses", portTypesWithSchemaClasses);
      if (failed.length() > 0) {
        json.put("failedOperators", failed);
      }
      apexCli.printJson(json);
    } finally {
      FileUtils.deleteDirectory(tmpDir);
    }
  }
}
