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

import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener;
import com.datatorrent.common.util.Pair;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.api.extensions.PluginContext;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.webapp.LogicalOperatorInfo;

public class DefaultPluginContextImpl implements PluginContext
{

  private final StreamingContainerManager dnmgr;
  private final Configuration launchConf;

  public DefaultPluginContextImpl(StreamingContainerManager dnmgr, Configuration launcConf)
  {
    this.dnmgr = dnmgr;
    this.launchConf = launcConf;
  }

  @Override
  public DAG getDAG()
  {
    return dnmgr.getLogicalPlan();
  }

  @Override
  public String getOperatorName(int id)
  {
    PTOperator ptOperator = dnmgr.getPhysicalPlan().getAllOperators().get(id);
    if (ptOperator != null) {
      return ptOperator.getName();
    }
    return null;
  }

  @Override
  public Configuration getLaunchConfig()
  {
    return launchConf;
  }

  @Override
  public StatsListener.BatchedOperatorStats getPhysicalOperatorStats(int id)
  {
    PTOperator ptOperator = dnmgr.getPhysicalPlan().getAllOperators().get(id);
    if (ptOperator != null) {
      return ptOperator.stats;
    }
    return null;
  }

  @Override
  public List<LogicalOperatorInfo> getLogicalOperatorInfoList()
  {
    return dnmgr.getLogicalOperatorInfoList();
  }

  @Override
  public Queue<Pair<Long, Map<String, Object>>> getWindowMetrics(String operatorName)
  {
    return dnmgr.getWindowMetrics(operatorName);
  }

  @Override
  public long windowIdToMillis(long windowId)
  {
    return dnmgr.windowIdToMillis(windowId);
  }
}
