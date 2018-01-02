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
/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.yarn.client;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedHashSet;

import org.apache.apex.engine.ClusterProviderFactory;
import org.apache.apex.engine.yarn.StramClient;
import org.apache.apex.engine.yarn.plan.logical.LogicalPlanAttributes;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.collect.Sets;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * Launch a streaming application packaged as jar file
 * <p>
 * Parses the jar file for application resources (implementations of {@link StreamingApplication} or property files per
 * naming convention).<br>
 * Dependency resolution is based on the bundled pom.xml (if any) and the application is launched with a modified client
 * classpath that includes application dependencies so that classes defined in the DAG can be loaded and
 * <br>
 *
 * @since 0.3.2
 */
public class StramAppLauncher extends com.datatorrent.stram.client.StramAppLauncher
{
  public StramAppLauncher(File appJarFile, Configuration conf) throws Exception
  {
    super(appJarFile, conf);
  }

  public StramAppLauncher(FileSystem fs, Path path, Configuration conf) throws Exception
  {
    super(fs, path, conf);
  }

  public StramAppLauncher(String name, Configuration conf) throws Exception
  {
    super(name, conf);
  }

  public StramAppLauncher(FileSystem fs, Configuration conf) throws Exception
  {
    super(fs, conf);
  }

  /**
   * Submit application to the cluster and return the app id.
   * Sets the context class loader for application dependencies.
   *
   * @param appConfig
   * @return ApplicationId
   * @throws Exception
   */
  public ApplicationId launchApp(AppFactory appConfig) throws Exception
  {
    loadDependencies();
    Configuration conf = propertiesBuilder.conf;
    conf.setEnum(StreamingApplication.ENVIRONMENT, StreamingApplication.Environment.CLUSTER);
    LogicalPlan dag = appConfig.createApp(propertiesBuilder);
    if (UserGroupInformation.isSecurityEnabled()) {
      long hdfsTokenMaxLifeTime = conf.getLong(StramClientUtils.DT_HDFS_TOKEN_MAX_LIFE_TIME, conf.getLong(StramClientUtils.HDFS_TOKEN_MAX_LIFE_TIME, StramClientUtils.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT));
      dag.setAttribute(LogicalPlan.HDFS_TOKEN_LIFE_TIME, hdfsTokenMaxLifeTime);
      long rmTokenMaxLifeTime = conf.getLong(StramClientUtils.DT_RM_TOKEN_MAX_LIFE_TIME, conf.getLong(YarnConfiguration.DELEGATION_TOKEN_MAX_LIFETIME_KEY, YarnConfiguration.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT));
      dag.setAttribute(LogicalPlanAttributes.RM_TOKEN_LIFE_TIME, rmTokenMaxLifeTime);
      setTokenRefreshCredentials(dag, conf);
    }
    String tokenRefreshFactor = conf.get(StramClientUtils.TOKEN_ANTICIPATORY_REFRESH_FACTOR);
    if (tokenRefreshFactor != null && tokenRefreshFactor.trim().length() > 0) {
      dag.setAttribute(LogicalPlan.TOKEN_REFRESH_ANTICIPATORY_FACTOR, Double.parseDouble(tokenRefreshFactor));
    }
    com.datatorrent.stram.StramClient client = ClusterProviderFactory.getProvider().getStramClient(conf, dag);
    try {
      client.start();
      LinkedHashSet<String> libjars = Sets.newLinkedHashSet();
      String libjarsCsv = conf.get(LIBJARS_CONF_KEY_NAME);
      if (libjarsCsv != null) {
        String[] jars = StringUtils.splitByWholeSeparator(libjarsCsv, com.datatorrent.stram.StramClient.LIB_JARS_SEP);
        libjars.addAll(Arrays.asList(jars));
      }
      if (deployJars != null) {
        for (File deployJar : deployJars) {
          libjars.add(deployJar.getAbsolutePath());
        }
      }

      client.setResources(libjars);
      client.setFiles(conf.get(FILES_CONF_KEY_NAME));
      client.setArchives(conf.get(ARCHIVES_CONF_KEY_NAME));
      client.setOriginalAppId(conf.get(ORIGINAL_APP_ID));
      client.setQueueName(conf.get(QUEUE_NAME));
      String tags = conf.get(TAGS);
      if (tags != null) {
        for (String tag : tags.split(",")) {
          client.addTag(tag.trim());
        }
      }
      client.startApplication();
      return ((StramClient)client).getApplicationReport().getApplicationId();
    } finally {
      client.stop();
    }
  }
}
