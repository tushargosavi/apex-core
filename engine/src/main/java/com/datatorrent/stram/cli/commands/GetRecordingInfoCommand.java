package com.datatorrent.stram.cli.commands;

import java.util.List;

import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.stram.cli.ApexCli;
import com.datatorrent.stram.client.RecordingsAgent;

import jline.console.ConsoleReader;

public class GetRecordingInfoCommand implements Command
{
  @Override
  public void execute(String[] args, ConsoleReader reader, ApexCli apexCli) throws Exception
  {
    if (args.length <= 1) {
      List<RecordingsAgent.RecordingInfo> recordingInfo = apexCli.getRecordingsAgent().getRecordingInfo(apexCli.getCurrentAppId());
      apexCli.printJson(recordingInfo, "recordings");
    } else if (args.length <= 2) {
      String opId = args[1];
      List<RecordingsAgent.RecordingInfo> recordingInfo = apexCli.getRecordingsAgent().getRecordingInfo(apexCli.getCurrentAppId(), opId);
      apexCli.printJson(recordingInfo, "recordings");
    } else {
      String opId = args[1];
      String id = args[2];
      RecordingsAgent.RecordingInfo recordingInfo = apexCli.getRecordingsAgent().getRecordingInfo(apexCli.getCurrentAppId(), opId, id);
      apexCli.printJson(new JSONObject(apexCli.getMapper().writeValueAsString(recordingInfo)));
    }
  }
}
