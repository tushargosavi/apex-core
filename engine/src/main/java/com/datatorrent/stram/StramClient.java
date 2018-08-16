/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.common.util.JarHelper;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * Submits application to YARN<p>
 * <br>
 *
 * @since 0.3.2
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class StramClient
{
  protected static final Logger LOG = LoggerFactory.getLogger(StramClient.class);
  public static final String APPLICATION_TYPE = "ApacheApex";
  @Deprecated
  public static final String APPLICATION_TYPE_DEPRECATED = "DataTorrent";

  protected static final String javaCmd = "${JAVA_HOME}" + "/bin/java";

  public static final String LIB_JARS_SEP = ",";

  // Configuration
  protected final Configuration conf;
  // Application master specific info to register a new Application with RM/ASM
  // App master priority
  protected final int amPriority = 0;
  protected final LogicalPlan dag;
  // log4j.properties file
  // if available, add to local resources and set into classpath
  protected final String log4jPropFile = "";
  // Timeout threshold for client. Kill app after time interval expires.
  protected long clientTimeout = 600000;
  protected String originalAppId;
  protected String queueName;
  protected String applicationType = APPLICATION_TYPE;
  protected String archives;
  protected String files;
  protected LinkedHashSet<String> resources;
  protected Set<String> tags = new HashSet<>();

  // platform dependencies that are not part of Hadoop and need to be deployed,
  // entry below will cause containing jar file from client to be copied to cluster
  protected static final Class<?>[] APEX_CLASSES = new Class<?>[]{
      com.datatorrent.netlet.util.Slice.class,
      com.datatorrent.netlet.EventLoop.class,
      com.datatorrent.bufferserver.server.Server.class,
      com.datatorrent.stram.StreamingAppMaster.class,
      com.datatorrent.api.StreamCodec.class,
      com.datatorrent.common.util.FSStorageAgent.class,
      javax.validation.ConstraintViolationException.class,
      com.esotericsoftware.kryo.Kryo.class,
      org.apache.bval.jsr303.ApacheValidationProvider.class,
      org.apache.bval.BeanValidationContext.class,
      org.apache.commons.lang3.ClassUtils.class,
      net.engio.mbassy.bus.MBassador.class,
      org.apache.apex.shaded.ning19.com.ning.http.client.ws.WebSocketUpgradeHandler.class,
      org.codehaus.jackson.annotate.JsonUnwrapped.class,
      org.codehaus.jackson.map.ser.std.RawSerializer.class,
      org.apache.commons.beanutils.BeanUtils.class,
      org.apache.http.client.utils.URLEncodedUtils.class,
      org.apache.http.message.BasicHeaderValueParser.class,
      com.esotericsoftware.minlog.Log.class,
      org.apache.xbean.asm5.tree.ClassNode.class,
      org.jctools.queues.SpscArrayQueue.class
  };

  protected static final Class<?>[] APEX_SECURITY_SPECIFIC_CLASSES = new Class<?>[]{
      com.sun.jersey.client.apache4.ApacheHttpClient4Handler.class
  };

  protected static final Class<?>[] APEX_SECURITY_CLASSES =
      (Class<?>[])ArrayUtils.addAll(APEX_CLASSES, APEX_SECURITY_SPECIFIC_CLASSES);

  public StramClient(Configuration conf, LogicalPlan dag) throws Exception
  {
    this.conf = conf;
    this.dag = dag;
    dag.validate();
  }

  public abstract void start();

  public abstract void stop();

  public LinkedHashSet<String> findJars(Class<?>[] defaultClasses)
  {
    List<Class<?>> jarClasses = new ArrayList<>();

    for (String className : dag.getClassNames()) {
      try {
        Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
        jarClasses.add(clazz);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Failed to load class " + className, e);
      }
    }

    for (Class<?> clazz : Lists.newArrayList(jarClasses)) {
      // process class and super classes (super does not require deploy annotation)
      for (Class<?> c = clazz; c != Object.class && c != null; c = c.getSuperclass()) {
        //LOG.debug("checking " + c);
        jarClasses.add(c);
        jarClasses.addAll(Arrays.asList(c.getInterfaces()));
      }
    }

    jarClasses.addAll(Arrays.asList(defaultClasses));

    if (dag.isDebug()) {
      LOG.debug("Deploy dependencies: {}", jarClasses);
    }

    LinkedHashSet<String> localJarFiles = new LinkedHashSet<>(); // avoid duplicates
    JarHelper jarHelper = new JarHelper();

    for (Class<?> jarClass : jarClasses) {
      localJarFiles.addAll(jarHelper.getJars(jarClass));
    }

    String libJarsPath = dag.getValue(Context.DAGContext.LIBRARY_JARS);
    if (!StringUtils.isEmpty(libJarsPath)) {
      String[] libJars = StringUtils.splitByWholeSeparator(libJarsPath, LIB_JARS_SEP);
      localJarFiles.addAll(Arrays.asList(libJars));
    }

    String pluginClassesPaths = conf.get(org.apache.apex.engine.api.StreamingAppMaster.PLUGINS_CONF_KEY);
    if (!StringUtils.isEmpty(pluginClassesPaths)) {
      for (String pluginClassPath : StringUtils.splitByWholeSeparator(pluginClassesPaths, org.apache.apex.engine.api.StreamingAppMaster.PLUGINS_CONF_SEP)) {
        try {
          localJarFiles.addAll(jarHelper.getJars(Thread.currentThread().getContextClassLoader().loadClass(pluginClassPath)));
        } catch (ClassNotFoundException ex) {
          LOG.error("Cannot find the class {}", pluginClassPath, ex);
        }
      }
    }

    LOG.info("Local jar file dependencies: {}", localJarFiles);

    return localJarFiles;
  }

  protected String copyFromLocal(FileSystem fs, Path basePath, String[] files) throws IOException
  {
    StringBuilder csv = new StringBuilder(files.length * (basePath.toString().length() + 16));
    for (String localFile : files) {
      Path src = new Path(localFile);
      String filename = src.getName();
      Path dst = new Path(basePath, filename);
      URI localFileURI = null;
      try {
        localFileURI = new URI(localFile);
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
      if (localFileURI.getScheme() == null || localFileURI.getScheme().startsWith("file")) {
        LOG.info("Copy {} from local filesystem to {}", localFile, dst);
        fs.copyFromLocalFile(false, true, src, dst);
      } else {
        LOG.info("Copy {} from DFS to {}", localFile, dst);
        FileUtil.copy(fs, src, fs, dst, false, true, conf);
      }
      if (csv.length() > 0) {
        csv.append(LIB_JARS_SEP);
      }
      csv.append(dst.toString());
    }
    return csv.toString();
  }

  public void copyInitialState(Path origAppDir) throws IOException
  {
    // locate previous snapshot
    long copyStart = System.currentTimeMillis();
    String newAppDir = this.dag.assertAppPath();

    FSRecoveryHandler recoveryHandler = new FSRecoveryHandler(origAppDir.toString(), conf);
    // read snapshot against new dependencies
    Object snapshot = recoveryHandler.restore();
    if (snapshot == null) {
      throw new IllegalArgumentException("No previous application state found in " + origAppDir);
    }
    InputStream logIs = recoveryHandler.getLog();

    // modify snapshot state to switch app id
    ((StreamingContainerManager.CheckpointState)snapshot).setApplicationId(this.dag, conf);
    Path checkpointPath = new Path(newAppDir, LogicalPlan.SUBDIR_CHECKPOINTS);

    FileSystem fs = FileSystem.newInstance(origAppDir.toUri(), conf);
    // remove the path that was created by the storage agent during deserialization and replacement
    fs.delete(checkpointPath, true);

    // write snapshot to new location
    recoveryHandler = new FSRecoveryHandler(newAppDir, conf);
    recoveryHandler.save(snapshot);
    OutputStream logOs = recoveryHandler.rotateLog();
    IOUtils.copy(logIs, logOs);
    logOs.flush();
    logOs.close();
    logIs.close();

    List<String> excludeDirs = Arrays.asList(LogicalPlan.SUBDIR_CHECKPOINTS, LogicalPlan.SUBDIR_EVENTS, LogicalPlan.SUBDIR_STATS);
    // copy sub directories that are not present in target
    FileStatus[] lFiles = fs.listStatus(origAppDir);

    // In case of MapR/MapR-FS, f.getPath().toString() returns path as maprfs:///<orig app dir>
    // whereas origAppDir.toString & newAppDir are in maprfs:/<orig or new app dir> format
    // e.g.
    // f.getPath().toString -> maprfs:///user/dtadmin/datatorrent/apps/application_1481890072066_0004/checkpoints
    // origAppDir -> maprfs:/user/dtadmin/datatorrent/apps/application_1481890072066_0004
    // newAppDir -> maprfs:/user/dtadmin/datatorrent/apps/application_1481890072066_0005

    String origAppDirPath = Path.getPathWithoutSchemeAndAuthority(origAppDir).toString();
    String newAppDirPath = Path.getPathWithoutSchemeAndAuthority(new Path(newAppDir)).toString();

    for (FileStatus f : lFiles) {
      if (f.isDirectory() && !excludeDirs.contains(f.getPath().getName())) {
        String targetPath = f.getPath().toString().replace(origAppDirPath, newAppDirPath);
        if (!fs.exists(new Path(targetPath))) {
          LOG.debug("Copying {} size {} to {}", f.getPath(), f.getLen(), targetPath);
          long start = System.currentTimeMillis();
          FileUtil.copy(fs, f.getPath(), fs, new Path(targetPath), false, conf);
          LOG.debug("Copying {} to {} took {} ms", f.getPath(), f.getLen(), targetPath, System.currentTimeMillis() - start);
        } else {
          LOG.debug("Ignoring {} as it already exists under {}", f.getPath(), targetPath);
        }
      }
    }
    LOG.info("Copying initial state took {} ms", System.currentTimeMillis() - copyStart);
  }

  /**
   * Launch application for the dag represented by this client.
   *
   * @throws Exception
   */
  public abstract void startApplication() throws Exception;

  public abstract void killApplication() throws Exception;

  public void setClientTimeout(long timeoutMillis)
  {
    this.clientTimeout = timeoutMillis;
  }

  /**
   * Monitor the submitted application for completion. Kill application if time expires.
   *
   * @return true if application completed successfully
   * @throws Exception
   */
  public abstract boolean monitorApplication() throws Exception;

  public void setApplicationType(String type)
  {
    this.applicationType = type;
  }

  public void setOriginalAppId(String appId)
  {
    this.originalAppId = appId;
  }

  public String getQueueName()
  {
    return queueName;
  }

  public void setQueueName(String queueName)
  {
    this.queueName = queueName;
  }

  public void addTag(String tag)
  {
    this.tags.add(tag);
  }

  public void setResources(LinkedHashSet<String> resources)
  {
    this.resources = resources;
  }

  public void setArchives(String archives)
  {
    this.archives = archives;
  }

  public void setFiles(String files)
  {
    this.files = files;
  }
}
