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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;

import com.google.common.collect.Lists;

import com.datatorrent.api.StatsListener;
import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;

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
    LOG.info("heartbeat received {}", hb);
  }

  public void dispatchEvent(StramEvent event)
  {
    poolExecutor.submit(new EventDiliveryTask(event));
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

  List<StatsListener> statsListeners = new ArrayList<>();

  @Override
  public void registerStatsListener(StatsListener listner)
  {
    statsListeners.add(listner);
  }

  List<ApexPlugin.EventListener> eventListeners = new ArrayList<>();

  @Override
  public void registerEventListener(ApexPlugin.EventListener listener)
  {
    eventListeners.add(listener);
  }

  @Override
  public void submit(Runnable task)
  {

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
      for (ApexPlugin.EventListener elistener : eventListeners) {
        elistener.handleEvent(event);
      }
    }
  }
}
