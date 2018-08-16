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
package org.apache.apex.engine.yarn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.apex.engine.yarn.security.ACLManager;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.datatorrent.api.Context;
import com.datatorrent.stram.IApexContainerLaunchContext;
import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.plan.logical.LogicalPlan;

public class ApexContainerLaunchContext implements IApexContainerLaunchContext
{
  private final Container container;
  private final NMClientAsync nmClient;
  private ContainerLaunchContext ctx;

  public ApexContainerLaunchContext(Container container, NMClientAsync nmClient)
  {
    this.container = container;
    this.nmClient = nmClient;
  }

  @Override
  public Configuration getConfig()
  {
    return nmClient.getConfig();
  }

  @Override
  public String getApplicationId()
  {
    return Integer.toString(container.getId().getApplicationAttemptId().getApplicationId().getId());
  }

  @Override
  public String getContainerIntegerId()
  {
    return Integer.toString(container.getId().getId());
  }

  @Override
  public String getContainerId()
  {
    return container.getId().toString();
  }

  @Override
  public String getNodeId()
  {
    return container.getNodeId().toString();
  }

  @Override
  public void startContainerAsync()
  {
    nmClient.startContainerAsync(container, ctx);
  }

  @Override
  public void initContainerLaunchContext()
  {
    ctx = Records.newRecord(ContainerLaunchContext.class);
  }

  @Override
  public void setEnvironment(Map<String, String> containerEnv)
  {
    ctx.setEnvironment(containerEnv);
  }

  @Override
  public void setTokens(ByteBuffer tokens)
  {
    ctx.setTokens(tokens);
  }

  @Override
  public void setLocalResources(LogicalPlan dag) throws IOException
  {
    Map<String, LocalResource> localResources = new HashMap<>();

    // add resources for child VM
    // child VM dependencies
    try (FileSystem fs = StramClientUtils.newFileSystemInstance(getConfig())) {
      addFilesToLocalResources(LocalResourceType.FILE, dag.getAttributes().get(Context.DAGContext.LIBRARY_JARS), localResources, fs);
      String archives = dag.getAttributes().get(LogicalPlan.ARCHIVES);
      if (archives != null) {
        addFilesToLocalResources(LocalResourceType.ARCHIVE, archives, localResources, fs);
      }
      ctx.setLocalResources(localResources);
    }
  }

  @Override
  public void setCommands(List<String> commands)
  {
    ctx.setCommands(commands);
  }

  @Override
  public void setupUserACLs(String launchUserName) throws IOException
  {
    ACLManager.setupUserACLs(ctx, launchUserName, getConfig());
  }

  public static void addFileToLocalResources(final String name, final FileStatus fileStatus, final LocalResourceType type, final Map<String, LocalResource> localResources)
  {
    final LocalResource localResource = LocalResource.newInstance(ConverterUtils.getYarnUrlFromPath(fileStatus.getPath()),
        type, LocalResourceVisibility.APPLICATION, fileStatus.getLen(), fileStatus.getModificationTime());
    localResources.put(name, localResource);
  }

  public static void addFilesToLocalResources(LocalResourceType type, String commaSeparatedFileNames, Map<String, LocalResource> localResources, FileSystem fs) throws IOException
  {
    String[] files = StringUtils.splitByWholeSeparator(commaSeparatedFileNames, com.datatorrent.stram.StramClient.LIB_JARS_SEP);
    for (String file : files) {
      final Path dst = new Path(file);
      addFileToLocalResources(dst.getName(), fs.getFileStatus(dst), type, localResources);
    }
  }
}
