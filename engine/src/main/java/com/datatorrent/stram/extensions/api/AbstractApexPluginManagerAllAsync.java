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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;
import com.datatorrent.stram.api.extensions.PluginLocator;

/**
 * A top level ApexPluginManager which will handle multiple requests through
 * a thread pool.
 */
public class AbstractApexPluginManagerAllAsync extends AbstractApexPluginManager
{
  protected transient ExecutorService poolExecutor;

  public AbstractApexPluginManagerAllAsync(PluginLocator locator, StramAppContext context, StreamingContainerManager dmgr)
  {
    super(locator, context, dmgr);
  }

  @Override
  public void dispatchHeartbeat(ContainerHeartbeat hb)
  {
    poolExecutor.submit(new HeartbeatDiliveryTask(hb));
  }

  @Override
  public void dispatchEvent(StramEvent event)
  {
    poolExecutor.submit(new EventDiliveryTask(event));
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception
  {
    super.serviceInit(conf);
    poolExecutor = Executors.newCachedThreadPool();
  }

  @Override
  protected void serviceStop() throws Exception
  {
    super.serviceStop();
    if (poolExecutor != null) {
      poolExecutor.shutdown();
      poolExecutor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  @Override
  public void submit(Runnable task)
  {
    poolExecutor.submit(task);
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
}
