package com.datatorrent.stram.cli.commands;

import javax.validation.constraints.NotNull;

import org.codehaus.jettison.json.JSONObject;

import com.google.common.base.Preconditions;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.webapp.TypeDiscoverer;

import jline.console.ConsoleReader;

public class ListDefaultAttributesCommand implements Command
{
  private final ApexCli.AttributesType type;

  public ListDefaultAttributesCommand(@NotNull ApexCli.AttributesType type)
  {
    this.type = Preconditions.checkNotNull(type);
  }

  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    JSONObject result;
    if (type == ApexCli.AttributesType.APPLICATION) {
      result = TypeDiscoverer.getAppAttributes();
    } else if (type == ApexCli.AttributesType.OPERATOR) {
      result = TypeDiscoverer.getOperatorAttributes();
    } else {
      //get port attributes
      result = TypeDiscoverer.getPortAttributes();
    }
    apexCli.printJson(result);
  }
}
