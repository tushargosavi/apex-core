/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.cli;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.ws.rs.core.MediaType;

import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.FileNameCompletor;
import jline.History;
import jline.MultiCompletor;
import jline.SimpleCompletor;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.stram.cli.StramClientUtils.ClientRMHelper;
import com.malhartech.stram.cli.StramClientUtils.YarnClientHelper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class StramCli {
  
  private static Logger LOG = LoggerFactory.getLogger(StramCli.class);
  
  private Configuration conf = new Configuration();
  private final ClientRMHelper rmClient;  
  private ApplicationReport currentApp = null;
  
  private class CliException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    CliException(String msg, Throwable cause) {
      super(msg, cause);
    }
    CliException(String msg) {
      super(msg);
    }
  }
  
  public StramCli() throws Exception {
    YarnClientHelper yarnClient = new YarnClientHelper(conf);
    rmClient = new ClientRMHelper(yarnClient);
  }
  
  public void init() {
  }

  public void run() throws IOException {
    printWelcomeMessage();
    ConsoleReader reader = new ConsoleReader();
    reader.setBellEnabled(false);

    String[] commandsList = new String[] { "help", "ls", "connect", "listnodes", "timeout", "kill", "exit" };
    List<Completor> completors = new LinkedList<Completor>();
    completors.add(new SimpleCompletor(commandsList));

    List<Completor> launchCompletors = new LinkedList<Completor>();
    launchCompletors.add(new SimpleCompletor("launch"));
    launchCompletors.add(new FileNameCompletor()); // jarFile
    launchCompletors.add(new FileNameCompletor()); // topology
    completors.add(new ArgumentCompletor(launchCompletors));
    
    reader.addCompletor(new MultiCompletor(completors));

    
    String historyFile = System.getProperty("user.home") + File.separator + ".history";
    try
    {
        History history = new History(new File(historyFile));
        reader.setHistory(history);
    }
    catch (IOException exp)
    {
        System.err.printf("Unable to open %s for writing.", historyFile);    
    }
    
    String line;
    PrintWriter out = new PrintWriter(System.out);

    while ((line = readLine(reader, "")) != null) {
      try {
        if ("help".equals(line)) {
          printHelp();
        } else if ("ls".equals(line)) {
          listApplications();
        } else if (line.startsWith("connect")) {
          connect(line);
        } else if (line.startsWith("listnodes")) {
          listNodes(line);
        } else if (line.startsWith("launch")) {
          launchApp(line, reader);
        } else if (line.startsWith("timeout")) {
          timeoutApp(line, reader);
        } else if (line.startsWith("kill")) {
          killApp(line);
        } else if ("exit".equals(line)) {
          System.out.println("Exiting application");
          return;
        } else {
          System.err
              .println("Invalid command, For assistance press TAB or type \"help\" then hit ENTER.");
        }
      } catch (CliException e) {
        System.err.println(e.getMessage());
        LOG.info("Error processing line: " + line, e);
      } catch (Exception e) {
        System.err.println("Unexpected error: " + e.getMessage());
        e.printStackTrace();
      }
      out.flush();
    }
  }

  private void printWelcomeMessage() {
    System.out
        .println("Stram CLI. For assistance press TAB or type \"help\" then hit ENTER.");
  }

  private void printHelp() {
    System.out.println("help             - Show help");
    System.out.println("ls               - Show running applications");
    System.out.println("connect <appId>  - Connect to running streaming application");
    System.out.println("listnodes        - List deployed streaming nodes");
    System.out.println("launch <jarFile> [<topologyFile>] - Launch topology packaged in jar file.");
    System.out.println("timeout <duration> - Wait for completion of current application.");
    System.out.println("kill             - Force termination for current application.");
    System.out.println("exit             - Exit the app");

  }

  private String readLine(ConsoleReader reader, String promtMessage)
      throws IOException {
    String line = reader.readLine(promtMessage + "\nstramcli> ");
    return line.trim();
  }

  private String[] assertArgs(String line, int num, String msg) {
    String[] args = StringUtils.splitByWholeSeparator(line, " ");
    if (args.length < num) {
      throw new CliException(msg);
    }
    return args;
  }
  
  private List<ApplicationReport> getApplicationList() {
    try {
      GetAllApplicationsRequest appsReq = Records.newRecord(GetAllApplicationsRequest.class);
      return rmClient.clientRM.getAllApplications(appsReq).getApplicationList();
    } catch (Exception e) {
      throw new CliException("Error getting application list from resource manager: " + e.getMessage(), e);
    }
  }
  
  private void listApplications() {
    try {
      List<ApplicationReport> appList = getApplicationList(); 
      System.out.println("Applications:");
      int totalCnt = 0;
      int runningCnt = 0;
      
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      
      for (ApplicationReport ar : appList) {
        StringBuilder sb = new StringBuilder();
        sb.append("startTime: " + sdf.format(new java.util.Date(ar.getStartTime())))
        .append(", id: " + ar.getApplicationId().getId())
        .append(", name: " + ar.getName())
        .append(", state: " + ar.getYarnApplicationState().name())
        .append(", trackingUrl: " + ar.getTrackingUrl())
        .append(", finalStatus: " + ar.getFinalApplicationStatus());
        System.out.println(sb);
        //Map<?,?> properties = BeanUtils.describe(ar);
        //System.out.println(properties);
        totalCnt++;
        if (ar.getYarnApplicationState() == YarnApplicationState.RUNNING) {
          runningCnt++;
        }
      }
      System.out.println(runningCnt + " active, total " + totalCnt + " applications.");
    } catch (Exception ex) {
      throw new CliException("Failed to retrieve application list", ex);
    }
  }

  private ClientResponse getResource(String resourcePath) {

    if (currentApp == null) {
      throw new CliException("No application selected");
    }
    
    Client wsClient = Client.create();
    wsClient.setFollowRedirects(true);
    WebResource r = wsClient.resource("http://" + currentApp.getTrackingUrl())
        .path("ws").path("v1").path("stram").path(resourcePath);
    try {
      ClientResponse response = r.accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      if (!MediaType.APPLICATION_JSON_TYPE.equals(response.getType())) {
        throw new Exception("Unexpected response type " + response.getType());
      }
      return response;
    } catch (Exception e) {
      throw new CliException("Failed to request " + r.getURI(), e);
    }
  }
  
  private void connect(String line) {
    String[] args = StringUtils.splitByWholeSeparator(line, " ");
    if (args.length != 2) {
      System.err.println("Invalid arguments");
      return;
    }

    int appSeq = Integer.parseInt(args[1]);
    
    List<ApplicationReport> appList = getApplicationList(); 
    for (ApplicationReport ar : appList) {
      if (ar.getApplicationId().getId() == appSeq) {
        currentApp = ar;
        break;
      }
    }
    if (currentApp == null) {
      throw new CliException("Invalid application id: " + args[1]);
    }

    try {
      LOG.info("Selected {} with tracking url: ", currentApp.getApplicationId(), currentApp.getTrackingUrl());
      ClientResponse rsp = getResource("info");
      JSONObject json = rsp.getEntity(JSONObject.class);      
      System.out.println(json.toString(2));
    } catch (Exception e) {
      currentApp = null;
      throw new CliException("Error connecting to app " + args[1], e);
    }
  }

  private void listNodes(String line) throws JSONException {

    ClientResponse rsp = getResource("nodes");
    JSONObject json = rsp.getEntity(JSONObject.class);      
    System.out.println(json.toString(2));
  }
  
  private void launchApp(String line, ConsoleReader reader) {
    String[] args = assertArgs(line, 2, "No jar file specified.");
    
    File tplgFile = null;
    if (args.length == 3) {
      tplgFile = new File(args[2]);
    }

    try {
      StramAppLauncher submitApp = new StramAppLauncher(new File(args[1]));
      
      if (tplgFile == null) {
        List<File> tplgList = submitApp.getBundledTopologies();
        if (tplgList.isEmpty()) {
          throw new CliException("No topology files bundled in jar, please specify one");
        }
        
        for (int i=0; i<tplgList.size(); i++) {
          System.out.printf("%3d. %s\n", i+1, tplgList.get(i));
        }

        boolean useHistory = reader.getUseHistory();
        reader.setUseHistory(false);
        @SuppressWarnings("unchecked")
        List<Completor> completors = new ArrayList<Completor>(reader.getCompletors());
        for (Completor c : completors) {
          reader.removeCompletor(c);
        }
        String optionLine = reader.readLine("Pick topology? ");
        reader.setUseHistory(useHistory);
        for (Completor c : completors) {
          reader.addCompletor(c);
        }

        try {
          int option = Integer.parseInt(optionLine);
          if (0 < option && option <= tplgList.size()) {
            tplgFile = tplgList.get(option-1);
          }
        } catch (Exception e) {
          // ignore
        }
      }

      if (tplgFile != null) {
        ApplicationId appId = submitApp.launchTopology(tplgFile);
        this.currentApp = rmClient.getApplicationReport(appId);
        System.out.println(appId);
      } else {
        System.err.println("No topology specified.");
      }
      
    } catch (Exception e) {
      throw new CliException("Failed to launch " + args[1] + " :" + e.getMessage(), e);
    }
    
  }

  private void killApp(String line) {
    if (currentApp == null) {
      throw new CliException("No application selected");
    }
    
    try {
      rmClient.killApplication(currentApp.getApplicationId());
    } catch (YarnRemoteException e) {
      throw new CliException("Failed to kill " + currentApp.getApplicationId(), e);
    }
  }

  private int getIntArg(String line, int argIndex, String msg) {
    String[] args = assertArgs(line, argIndex+1, msg);
    try {
      int arg = Integer.parseInt(args[argIndex]);
      return arg;
    } catch (Exception e) {
      throw new CliException("Not a valid number: " + args[argIndex]);
    }
  }
  
  private void timeoutApp(String line, final ConsoleReader reader) {
    if (currentApp == null) {
      throw new CliException("No application selected");
    }
    int timeout = getIntArg(line, 1, "Specify wait duration");

    ClientRMHelper.AppStatusCallback cb = new ClientRMHelper.AppStatusCallback() {
      @Override
      public boolean exitLoop(ApplicationReport report) {
        System.out.println("current status is: " + report.getYarnApplicationState());
        try {
          if (reader.getInput().available() > 0) {
            return true;
          }
        } catch (IOException e) {
          LOG.error("Error checking for input.", e);
        }
        return false;
      }
    };
      
    try {
      boolean result = rmClient.waitForCompletion(currentApp.getApplicationId(), cb, timeout*1000);
      if (!result) {
        System.err.println("Application terminated unsucessful.");
      }
    } catch (YarnRemoteException e) {
      throw new CliException("Failed to kill " + currentApp.getApplicationId(), e);
    }
    
  }
  
  public static void main(String[] args) throws Exception {
    StramCli shell = new StramCli();
    shell.init();
    shell.run();
  }
}
