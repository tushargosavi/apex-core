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
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener.BatchedOperatorStats;
import com.datatorrent.common.util.Pair;
import com.datatorrent.netlet.util.CircularBuffer;
import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;
import com.datatorrent.stram.api.extensions.ApexPlugin;
import com.datatorrent.stram.api.extensions.PluginContext;
import com.datatorrent.stram.api.extensions.PluginLocator;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.webapp.LogicalOperatorInfo;

/**
 * A top level ApexPluginManager which will handle multiple requests through
 * a thread pool.
 */
public class ApexPluginManagerWithThreadPerPlugin extends AbstractApexPluginManagerAllAsync
{
  private static final Logger LOG = LoggerFactory.getLogger(ApexPluginManagerWithThreadPerPlugin.class);

  public ApexPluginManagerWithThreadPerPlugin(PluginLocator locator, StramAppContext context, StreamingContainerManager dmgr)
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

  private List<PluginExecutionContext> pluginExecutors = new ArrayList<>();

  @Override
  protected void serviceInit(Configuration conf) throws Exception
  {
    super.serviceInit(conf);
    for (ApexPlugin plugin : userServices) {
      LOG.info("starting executor for plugin {}", plugin);
      PluginInfo pInfo = getPluginInfo(plugin);
      PluginExecutionContext context = new PluginExecutionContext(pInfo);
      pluginExecutors.add(new PluginExecutionContext(pInfo));
      context.start();
    }
  }

  @Override
  protected void serviceStop() throws Exception
  {
    super.serviceStop();
    for (PluginExecutionContext ctx : pluginExecutors) {
      ctx.stop();
    }
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
      LOG.info("submitting heartbeat scheule task");
      for (PluginExecutionContext ctx : pluginExecutors) {
        LOG.info("plugin has registered for heartbeat {} {}", ctx.pInfo.plugin, ctx.pInfo.heartbeatHandler != null);
        if (ctx.pInfo.heartbeatHandler != null) {
          LOG.info("submitting heartbeat task on thread ");
          ctx.blockingQueue.offer(heartbeat);
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
      for (final PluginExecutionContext ctx : pluginExecutors) {
        if (ctx.pInfo.eventHandler != null) {
          LOG.info("submitting event task on thread ");
          ctx.blockingQueue.offer(event);
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

  class PluginExecutionContext implements Runnable
  {
    private final PluginInfo pInfo;
    private Exception error;
    private Thread thread;
    private volatile boolean alive = false;
    int qsize = 1024;
    private final CircularBuffer blockingQueue = new CircularBuffer(qsize);

    PluginExecutionContext(PluginInfo pInfo)
    {
      this.pInfo = pInfo;
    }

    public void run()
    {
      alive = true;
      while (alive) {
        Object item = blockingQueue.peek();
        if (item == null) {
          try {
            Thread.sleep(200);
            // TODO add idle handler for thread
          } catch (InterruptedException e) {
            if (!alive) {
              break;
            } else {
              // Make plugin inactive.
            }
          }
        }
        item = blockingQueue.poll();
        if (item == null) {
          continue;
        }
        // Any exception while processing an item, will cause plugin to deactivate
        try {
          if (item instanceof ContainerHeartbeat) {
            pInfo.heartbeatHandler.handle((ContainerHeartbeat)item);
          } else if (item instanceof StramEvent) {
            pInfo.eventHandler.handle((StramEvent)item);
          }
        } catch (Exception ex) {
          LOG.warn("Exception in plugin execution {}", ex);
          alive = false;
          error = ex;
        }
      }
    }

    void start()
    {
      thread = new Thread(this);
      thread.start();
    }

    void stop() throws InterruptedException
    {
      alive = false;
      if (thread != null) {
        thread.join();
        thread = null;
      }
    }
  }
}
