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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.stram.api.StramEvent;

/**
 * A s
 */
public class DebugApexService implements ApexPlugin
{
  private static final Logger LOG = LoggerFactory.getLogger(DebugApexService.class);

  @Override
  public void init(PluginManager manager)
  {
    manager.registerEventListener(new DebugEventListener());
    manager.registerStatsListener(new DebugStatsListener());
  }

  @Override
  public void shutdown()
  {

  }

  class DebugStatsListener implements StatsListener
  {
    @Override
    public Response processStats(BatchedOperatorStats stats)
    {
      for (Stats.OperatorStats ostats : stats.getLastWindowedStats()) {
        LOG.info("received stats {}", ostats);
      }
      return null;
    }
  }

  class DebugEventListener implements ApexPlugin.EventListener
  {
    @Override
    public void handleEvent(StramEvent event)
    {
      LOG.info("StramEvent occured {} reason {}", event, event.getReason());
    }
  }
}
