package com.datatorrent.stram.cli.commands;

import java.util.Iterator;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import org.apache.commons.lang.StringUtils;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.webapp.StramWebServices;

import jline.console.ConsoleReader;

public class ListOperatorsCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    JSONObject json = apexCli.getCurrentAppResource(StramWebServices.PATH_PHYSICAL_PLAN_OPERATORS);

    if (args.length > 1) {
      String singleKey = "" + json.keys().next();
      JSONArray matches = new JSONArray();
      // filter operators
      JSONArray arr;
      Object obj = json.get(singleKey);
      if (obj instanceof JSONArray) {
        arr = (JSONArray)obj;
      } else {
        arr = new JSONArray();
        arr.put(obj);
      }
      for (int i = 0; i < arr.length(); i++) {
        JSONObject oper = arr.getJSONObject(i);
        if (StringUtils.isNumeric(args[1])) {
          if (oper.getString("id").equals(args[1])) {
            matches.put(oper);
            break;
          }
        } else {
          @SuppressWarnings("unchecked")
          Iterator<String> keys = oper.keys();
          while (keys.hasNext()) {
            if (oper.get(keys.next()).toString().toLowerCase().contains(args[1].toLowerCase())) {
              matches.put(oper);
              break;
            }
          }
        }
      }
      json.put(singleKey, matches);
    }

    apexCli.printJson(json);
  }
}

