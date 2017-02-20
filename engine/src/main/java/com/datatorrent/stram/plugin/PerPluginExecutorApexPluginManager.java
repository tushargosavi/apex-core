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
package com.datatorrent.stram.plugin;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;
import com.datatorrent.stram.api.plugin.ApexPlugin;
import com.datatorrent.stram.api.plugin.PluginLocator;

/**
 * A top level ApexPluginManager which will handle multiple requests through
 * a thread pool.
 */
public class PerPluginExecutorApexPluginManager extends AbstractApexPluginManagerAllAsync
{
  private static final Logger LOG = LoggerFactory.getLogger(PerPluginExecutorApexPluginManager.class);

  public PerPluginExecutorApexPluginManager(PluginLocator locator, StramAppContext context, StreamingContainerManager dmgr)
  {
    super(locator, context, dmgr);
  }

  @Override
  public void dispatchHeartbeat(final ContainerHeartbeat hb)
  {
    for (final PluginInfo pInfo : plugins.values()) {
      if (pInfo.heartbeatHandler != null) {
        PluginExecutionContext ctx = pluginExecutors.get(pInfo);
        ctx.executorService.submit(new Runnable()
        {
          @Override
          public void run()
          {
            pInfo.heartbeatHandler.handle(hb);
          }
        });
      }
    }
  }

  @Override
  public void dispatchEvent(final StramEvent event)
  {
    for (final PluginInfo pInfo : plugins.values()) {
      if (pInfo.eventHandler != null) {
        PluginExecutionContext ctx = pluginExecutors.get(pInfo);
        if (ctx == null || ctx.executorService == null) {
          LOG.warn("plugin context is set to null for plugin {}", pInfo.plugin);
          continue;
        }
        ctx.executorService.submit(new Runnable()
        {
          @Override
          public void run()
          {
            pInfo.eventHandler.handle(event);
          }
        });
      }
    }
  }

  private Map<PluginInfo, PluginExecutionContext> pluginExecutors = new HashMap<>();

  @Override
  protected void serviceInit(Configuration conf) throws Exception
  {
    super.serviceInit(conf);
    for (ApexPlugin plugin : userServices) {
      LOG.info("starting executor for plugin {}", plugin);
      PluginInfo pInfo = getPluginInfo(plugin);
      PluginExecutionContext context = new PluginExecutionContext(pInfo);
      pluginExecutors.put(pInfo, context);
      context.start();
    }
  }

  @Override
  protected void serviceStop() throws Exception
  {
    super.serviceStop();
    for (PluginExecutionContext ctx : pluginExecutors.values()) {
      ctx.stop();
    }
  }

  class PluginExecutionContext
  {
    private final PluginInfo pInfo;
    ExecutorService executorService;
    int qsize = 4096;
    private BlockingQueue blockingQueue;

    PluginExecutionContext(PluginInfo pInfo)
    {
      this.pInfo = pInfo;
    }

    void start()
    {
      blockingQueue = new ArrayBlockingQueue<>(qsize);
      RejectedExecutionHandler handler = new RejectedExecutionHandler()
      {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
        {
          try {
            Object item = blockingQueue.remove();
            executor.submit(r);
          } catch (NoSuchElementException ex) {
            // Ignore no-such element as queue may finish, while this handler is called.
          }
        }
      };

      executorService = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, blockingQueue, handler);
    }

    void stop() throws InterruptedException
    {
      if (executorService != null) {
        executorService.shutdownNow();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
      }
    }
  }
}
