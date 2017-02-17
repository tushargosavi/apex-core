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
package com.datatorrent.stram.extensions.api;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.collect.Lists;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener.BatchedOperatorStats;
import com.datatorrent.common.util.Pair;
import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;
import com.datatorrent.stram.api.extensions.ApexPlugin;
import com.datatorrent.stram.api.extensions.PluginContext;
import com.datatorrent.stram.api.extensions.PluginLocator;
import com.datatorrent.stram.api.extensions.PluginManager;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.webapp.LogicalOperatorInfo;

/**
 * A top level ApexPluginManager which will handle multiple requests through
 * a thread pool.
 */
public class ApexPluginManager extends CompositeService implements PluginManager
{
  private static final Logger LOG = LoggerFactory.getLogger(ApexPluginManager.class);
  private Collection<ApexPlugin> userServices = Lists.newArrayList();
  private transient ExecutorService poolExecutor;
  private final StramAppContext appContext;
  private final StreamingContainerManager dmgr;
  private final PluginLocator locator;
  private transient Configuration launchConfig;
  private FileContext fileContext;

  public ApexPluginManager(PluginLocator locator, StramAppContext context, StreamingContainerManager dmgr)
  {
    super(ApexPluginManager.class.getName());
    this.locator = locator;
    this.appContext = context;
    this.dmgr = dmgr;
    LOG.info("Creating appex service ");
  }

  public void dispatchStats(ContainerHeartbeat hb)
  {
    poolExecutor.submit(new HeartbeatDiliveryTask(hb));
  }

  public void dispatchEvent(StramEvent event)
  {
    poolExecutor.submit(new EventDiliveryTask(event));
  }

  private Configuration readLaunchConfiguration() throws IOException
  {
    LOG.info("Reading configuration information ");
    Path appPath = new Path(appContext.getApplicationPath());
    URI uri = appPath.toUri();
    Configuration config = new YarnConfiguration();
    fileContext = uri.getScheme() == null ? FileContext.getFileContext(config) : FileContext.getFileContext(uri, config);
    FSDataInputStream is = fileContext.open(new Path(appPath, LogicalPlan.LAUNCH_CONFIG_FILE_NAME));
    config.addResource(is);
    LOG.info("read launch configuration ");
    return config;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception
  {
    super.serviceInit(conf);
    if (locator != null) {
      Collection<ApexPlugin> plugins = locator.discoverPlugins();
      if (plugins != null) {
        userServices.addAll(plugins);
      }
    }

    LOG.info("ApexPluginManager called ");
    super.serviceStart();
    for (ApexPlugin plugin : userServices) {
      plugin.init(this);
    }
    this.launchConfig = readLaunchConfiguration();
    poolExecutor = Executors.newCachedThreadPool();
  }


  @Override
  protected void serviceStop() throws Exception
  {
    super.serviceStop();
    if (poolExecutor != null) {
      poolExecutor.shutdown();
    }
  }

  class PluginInfo
  {
    final ApexPlugin plugin;
    Set<RegistrationType<?>> registrations = new HashSet<>();

    Handler<ContainerHeartbeat> heartbeatHandler;
    Handler<StramEvent> eventHandler;

    public PluginInfo(ApexPlugin plugin)
    {
      this.plugin = plugin;
    }

  }

  private Map<ApexPlugin, PluginInfo> plugins = new HashMap<>();

  PluginInfo getPluginInfo(ApexPlugin plugin)
  {
    PluginInfo pInfo = plugins.get(plugin);
    if (pInfo == null) {
      pInfo = new PluginInfo(plugin);
      plugins.put(plugin, pInfo);
    }
    return pInfo;
  }

  @Override
  public <T> void register(RegistrationType<T> type, Handler<T> handler, ApexPlugin owner)
  {
    PluginInfo pInfo = getPluginInfo(owner);
    boolean fresh = pInfo.registrations.add(type);
    if (!fresh) {
      LOG.warn("Handler already registered for the event type {} by plugin {}", type, owner);
      return;
    }
    if (type == HEARTBEAT) {
      pInfo.heartbeatHandler = (Handler<ContainerHeartbeat>)handler;
    } else if (type == STRAM_EVENT) {
      pInfo.eventHandler = (Handler<StramEvent>)handler;
    }
  }

  public void submit(Runnable task)
  {
    poolExecutor.submit(task);
  }

  @Override
  public StramAppContext getApplicationContext()
  {
    return null;
  }

  @Override
  public PluginContext getPluginContext()
  {
    return null;
  }

  @Override
  public void setup()
  {
  }

  @Override
  public void shutdown()
  {

  }

  public void addPlugin(ApexPlugin obj)
  {
    userServices.add(obj);
  }

  private class HeartbeatDiliveryTask implements Runnable
  {
    private final ContainerHeartbeat heartbeat;
    public HeartbeatDiliveryTask(ContainerHeartbeat hb)
    {
      this.heartbeat = hb;
    }

    @Override
    public void run()
    {
      for (PluginInfo pInfo : plugins.values()) {
        if (pInfo.heartbeatHandler != null) {
          pInfo.heartbeatHandler.handle(heartbeat);
        }
      }
    }
  }

  private class EventDiliveryTask implements Runnable
  {
    private final StramEvent event;
    public EventDiliveryTask(StramEvent event)
    {
      this.event = event;
    }

    @Override
    public void run()
    {
      for (PluginInfo pInfo : plugins.values()) {
        if (pInfo.eventHandler != null) {
          pInfo.eventHandler.handle(event);
        }
      }
    }
  }

  class DefaultPluginContext implements PluginContext
  {
    @Override
    public String getOperatorName(int id)
    {
      return null;
    }

    public DAG getDAG()
    {
      return dmgr.getLogicalPlan();
    }

    public BatchedOperatorStats getPhysicalOperatorStats(int id)
    {
      PTOperator operator = dmgr.getPhysicalPlan().getAllOperators().get(id);
      if (operator != null) {
        return operator.stats;
      }
      return null;
    }

    public List<LogicalOperatorInfo> getLogicalOperatorInfoList()
    {
      return dmgr.getLogicalOperatorInfoList();
    }

    public Queue<Pair<Long, Map<String, Object>>> getWindowMetrics(String operatorName)
    {
      return dmgr.getWindowMetrics(operatorName);
    }

    public long windowIdToMillis(long windowId)
    {
      return dmgr.windowIdToMillis(windowId);
    }

    @Override
    public Configuration getLaunchConfig()
    {
      return launchConfig;
    }
  }
}
