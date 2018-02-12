/**
 * Copyright (c) 2012-2018 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.k8s;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashSet;

import org.codehaus.jettison.json.JSONObject;

import org.apache.apex.engine.ClusterProviderFactory;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarOutputStream;

import com.google.common.base.MoreObjects;

import com.datatorrent.api.Context;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.BasicContainerOptConfigurator;
import com.datatorrent.stram.StramUtils;
import com.datatorrent.stram.StreamingAppMaster;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * Created by sergey on 2/2/18.
 */
public class StramClient extends com.datatorrent.stram.StramClient
{
  protected String appId;

  protected static final Class<?>[] EXTRA_CLASSES = new Class<?>[]{
      com.google.common.base.Strings.class,
      org.apache.hadoop.conf.Configuration.class,
      org.apache.commons.logging.LogFactory.class,
      org.apache.commons.collections.map.UnmodifiableMap.class,
      org.slf4j.LoggerFactory.class
  };

  private static final String APP_FOLDER_NAME = "/app";

  public StramClient(Configuration conf, LogicalPlan dag) throws Exception
  {
    super(conf, dag);
  }

  @Override
  public void start()
  {

  }

  @Override
  public void stop()
  {

  }

  @Override
  public void startApplication() throws Exception
  {
    // Get a new application id and make tmp application folder for docker image files

    String appName = dag.getValue(LogicalPlan.APPLICATION_NAME);
    String userName = UserGroupInformation.getLoginUser().getUserName();
    appId = (userName + "_" + appName).toLowerCase() + "_" + new Date().getTime();
    String localTmpFolder = System.getProperty("java.io.tmpdir") + appId;
    new File(localTmpFolder).mkdirs();

    // buffer for body of a docker file
    StringBuffer dockerFileBody = new StringBuffer("FROM openjdk:latest\n");

    // -------------------------- store all jars to jars.tar  --------------------------

    Class<?>[] defaultClasses;
    if (applicationType.equals(APPLICATION_TYPE)) {
      if (UserGroupInformation.isSecurityEnabled()) {
        defaultClasses = APEX_SECURITY_CLASSES;
      } else {
        defaultClasses = APEX_CLASSES;
      }
    } else {
      throw new IllegalStateException(applicationType + " is not a valid application type.");
    }

    defaultClasses = (Class<?>[])ArrayUtils.addAll(defaultClasses, EXTRA_CLASSES);
    LinkedHashSet<String> localJarFiles = findJars(defaultClasses);

    String[] jars = addFilesToTarFile(localTmpFolder + "/jars.tar", localJarFiles);
    dockerFileBody.append("ADD jars.tar " + APP_FOLDER_NAME + "\n");

    // TODO: ??? setupSSLResources(dag.getValue(Context.DAGContext.SSL_CONFIG), fs, appPath, localResources);

    // -------------------------- add properties to logical plan  --------------------------

    if (dag.getAttributes().get(LogicalPlan.APPLICATION_ID) == null) {
      dag.setAttribute(LogicalPlan.APPLICATION_ID, appId);
    }

    dag.getAttributes().put(LogicalPlan.APPLICATION_PATH, APP_FOLDER_NAME);
    StorageAgent agent = dag.getAttributes().get(Context.OperatorContext.STORAGE_AGENT);
    if (agent != null && agent instanceof StorageAgent.ApplicationAwareStorageAgent) {
      ((StorageAgent.ApplicationAwareStorageAgent)agent).setApplicationAttributes(dag.getAttributes());
    }

    if (dag.getAttributes().get(Context.OperatorContext.STORAGE_AGENT) == null) { /* which would be the most likely case */
      Path checkpointPath = new Path(APP_FOLDER_NAME, LogicalPlan.SUBDIR_CHECKPOINTS);
      // use conf client side to pickup any proxy settings from dt-site.xml
      dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new AsyncFSStorageAgent(checkpointPath.toString(), conf));
    }

    if (dag.getAttributes().get(LogicalPlan.CONTAINER_OPTS_CONFIGURATOR) == null) {
      dag.setAttribute(LogicalPlan.CONTAINER_OPTS_CONFIGURATOR, new BasicContainerOptConfigurator());
    }

    LOG.info("Set the environment for the application master");

    // -------------------------- store logical plan and application context properties to logicalPlan.tar  --------------------------

    int amMemory = dag.getMasterMemoryMB();

    // store logical plan to the tmp application folder
    String serFile = localTmpFolder + "/" + LogicalPlan.SER_FILE_NAME;
    FileOutputStream outStream = new FileOutputStream(serFile);
    LogicalPlan.write(this.dag, outStream);
    outStream.close();

    // store application context properties to the tmp application folder
    JSONObject appContext = new JSONObject();
    appContext.put("appId", appId);
    appContext.put("appName", appName);
    appContext.put("appType", applicationType);
    appContext.put("memory", amMemory);
    PrintWriter writer = new PrintWriter(localTmpFolder + "/" + "appContext.json", "UTF-8");
    writer.print(appContext.toString());
    writer.close();

    addFilesToTarFile(localTmpFolder + "/logicalPlan.tar", Arrays.asList(serFile, localTmpFolder + "/appContext.json"));
    dockerFileBody.append("ADD logicalPlan.tar" + " /app\n");

    // -------------------------- add jars to classpath and env variables --------------------------

    StringBuilder classPathEnv = new StringBuilder("./*");
    for (String jar : jars) {
      classPathEnv.append(':');
      classPathEnv.append(APP_FOLDER_NAME + "/" + jar);
    }
    dockerFileBody.append("ENV JAVA_CLASSPATH " + classPathEnv.toString() + "\n");
    dockerFileBody.append("ENV USER_NAME " + userName);

    // -------------------------- make java executable command  --------------------------

    ArrayList<CharSequence> vargs = new ArrayList<>(30);

    LOG.info("Setting up app master command");

    if (dag.isDebug()) {
      vargs.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n");
    }
    // Set Xmx based on am memory size
    // default heap size 75% of total memory
    if (dag.getMasterJVMOptions() != null) {
      vargs.add(dag.getMasterJVMOptions());
    }

    vargs.add("-Xmx" + (amMemory * 3 / 4) + "m");
    vargs.add("-XX:+HeapDumpOnOutOfMemoryError");
//    vargs.add("-XX:HeapDumpPath=" + System.getProperty("java.io.tmpdir") + "/dt-heap-" + appId + ".bin");

    vargs.add(String.format("-D%s='%s'", LogicalPlan.APPLICATION_NAME.getLongName(), appName));
    vargs.add(String.format("-D%s='%s'", LogicalPlan.LOGGER_APPENDER.getLongName(), dag.getValue(LogicalPlan.LOGGER_APPENDER)));
    if (dag.isDebug()) {
      vargs.add("-Dlog4j.debug=true");
    }

    // TODO: log4j configuration
//    vargs.add(String.format("-D%s=%s", "apex.logger.appender",
//        "stdout;log4j.appender.stdout=org.apache.log4j.ConsoleAppender,log4j.appender.stdout.Target=System.out,log4j.appender.stdout.layout=org.apache.log4j.PatternLayout"));

    String loggersLevel = conf.get(StramUtils.DT_LOGGERS_LEVEL);
    if (loggersLevel != null) {
      vargs.add(String.format("-D%s=%s", StramUtils.DT_LOGGERS_LEVEL, loggersLevel));
    }
    vargs.add(String.format("-D%s=%s", ClusterProviderFactory.PROPERTY_PROVIDER_TYPE, ClusterProviderFactory.K8S_PROVIDER_TYPE));
    vargs.add("-cp ${JAVA_CLASSPATH}");
    vargs.add(StreamingAppMaster.class.getName());
//    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
//    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

    StringBuilder command = new StringBuilder(9 * vargs.size());
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    dockerFileBody.append("CMD java " + command + "\n");
    dockerFileBody.append("EXPOSE 8080\n");

    // -------------------------- make Docker file  --------------------------
    // make Dockerfile

    writer = new PrintWriter(localTmpFolder + "/" + "Dockerfile", "UTF-8");
    writer.println(dockerFileBody.toString());
    writer.close();

    LOG.info(MoreObjects.toStringHelper("Submitting application: ")
        .add("name", appContext.getString("appName"))
        .add("user", userName)
        .add("docker", dockerFileBody.toString())
        .toString());

    // TODO: Launch application
  }

  public String[] addFilesToTarFile(String targetFileName, Collection<String> files) throws IOException
  {
    File targetFile = new File(targetFileName);
    targetFile.createNewFile();

    FileOutputStream dest = new FileOutputStream(targetFile);
    TarOutputStream out = new TarOutputStream(new BufferedOutputStream(dest));
    String[] fileNames = new String[files.size()];
    int index = 0;

    for (String fileName : files) {
      File file = new File(fileName);
      TarEntry tarEntry = new TarEntry(file, file.getName());
      out.putNextEntry(tarEntry);
      BufferedInputStream origin = new BufferedInputStream(new FileInputStream(file));
      fileNames[index++] = file.getName();

      int count;
      byte[] data = new byte[2048];
      while ((count = origin.read(data)) != -1) {
        out.write(data, 0, count);
      }

      origin.close();
      out.closeEntry();
      out.flush();
    }

    out.close();

    return fileNames;
  }

  @Override
  public void killApplication() throws Exception
  {

  }

  @Override
  public boolean monitorApplication() throws Exception
  {
    return false;
  }

  public String getAppId()
  {
    return appId;
  }
}
