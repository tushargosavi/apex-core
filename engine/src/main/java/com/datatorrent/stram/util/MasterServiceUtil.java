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
package com.datatorrent.stram.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.bind.annotation.XmlElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.ClusterProviderFactory;
import org.apache.apex.engine.api.Settings;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.api.AppDataSource;
import com.datatorrent.stram.api.BaseContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.OperatorStatus.PortStatus;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.webapp.AppInfo;

/**
 * Streaming Application Master
 *
 * @since 0.3.2
 */
public class MasterServiceUtil
{
  protected static final Logger LOG = LoggerFactory.getLogger(MasterServiceUtil.class);

  public static class NodeFailureStats
  {
    private long lastFailureTimeStamp;
    private int failureCount;
    private long blackListAdditionTime;

    public NodeFailureStats(long lastFailureTimeStamp, int failureCount)
    {
      this.lastFailureTimeStamp = lastFailureTimeStamp;
      this.failureCount = failureCount;
    }

    public long getLastFailureTimeStamp()
    {
      return lastFailureTimeStamp;
    }

    public void setLastFailureTimeStamp(long lastFailureTimeStamp)
    {
      this.lastFailureTimeStamp = lastFailureTimeStamp;
    }

    public int getFailureCount()
    {
      return failureCount;
    }

    public void setFailureCount(int failureCount)
    {
      this.failureCount = failureCount;
    }

    public void IncFailureCount()
    {
      this.failureCount++;
    }

    public long getBlackListAdditionTime()
    {
      return blackListAdditionTime;
    }

    public void setBlackListAdditionTime(long blackListAdditionTime)
    {
      this.blackListAdditionTime = blackListAdditionTime;
    }
  }

  /**
   * Overrides getters to pull live info.
   */
  public static class ClusterAppStats extends AppInfo.AppStats
  {
    private transient StreamingContainerManager dnmgr;
    // Containers that the RM has allocated to us
    private final transient ConcurrentMap<String, IAllocatedContainer> allocatedContainers = Maps.newConcurrentMap();
    // Count of failed containers
    private final transient AtomicInteger numFailedContainers = new AtomicInteger();

    public void setDnmgr(StreamingContainerManager dnmgr)
    {
      this.dnmgr = dnmgr;
    }

    public StreamingContainerManager fetchDnmgr()
    {
      return dnmgr;
    }

    public Map<String, IAllocatedContainer> fetchAllocatedContainersMap()
    {
      return allocatedContainers;
    }

    public AtomicInteger fetchNumFailedContainers()
    {
      return numFailedContainers;
    }

    @AutoMetric
    @Override
    public int getAllocatedContainers()
    {
      return allocatedContainers.size();
    }

    @AutoMetric
    @Override
    public int getPlannedContainers()
    {
      return dnmgr.getPhysicalPlan().getContainers().size();
    }

    @AutoMetric
    @Override
    @XmlElement
    public int getFailedContainers()
    {
      return numFailedContainers.get();
    }

    @AutoMetric
    @Override
    public int getNumOperators()
    {
      return dnmgr.getPhysicalPlan().getAllOperators().size();
    }

    @Override
    public long getCurrentWindowId()
    {
      long min = Long.MAX_VALUE;
      for (Map.Entry<Integer, PTOperator> entry : dnmgr.getPhysicalPlan().getAllOperators().entrySet()) {
        long windowId = entry.getValue().stats.currentWindowId.get();
        if (min > windowId) {
          min = windowId;
        }
      }
      return StreamingContainerManager.toWsWindowId(min == Long.MAX_VALUE ? 0 : min);
    }

    @Override
    public long getRecoveryWindowId()
    {
      return StreamingContainerManager.toWsWindowId(dnmgr.getCommittedWindowId());
    }

    @AutoMetric
    @Override
    public long getTuplesProcessedPSMA()
    {
      long result = 0;
      for (Map.Entry<Integer, PTOperator> entry : dnmgr.getPhysicalPlan().getAllOperators().entrySet()) {
        result += entry.getValue().stats.tuplesProcessedPSMA.get();
      }
      return result;
    }

    @AutoMetric
    @Override
    public long getTotalTuplesProcessed()
    {
      long result = 0;
      for (Map.Entry<Integer, PTOperator> entry : dnmgr.getPhysicalPlan().getAllOperators().entrySet()) {
        result += entry.getValue().stats.totalTuplesProcessed.get();
      }
      return result;
    }

    @AutoMetric
    @Override
    public long getTuplesEmittedPSMA()
    {
      long result = 0;
      for (Map.Entry<Integer, PTOperator> entry : dnmgr.getPhysicalPlan().getAllOperators().entrySet()) {
        result += entry.getValue().stats.tuplesEmittedPSMA.get();
      }
      return result;
    }

    @AutoMetric
    @Override
    public long getTotalTuplesEmitted()
    {
      long result = 0;
      for (Map.Entry<Integer, PTOperator> entry : dnmgr.getPhysicalPlan().getAllOperators().entrySet()) {
        result += entry.getValue().stats.totalTuplesEmitted.get();
      }
      return result;
    }

    @AutoMetric
    @Override
    public long getTotalMemoryAllocated()
    {
      long result = 0;
      for (PTContainer c : dnmgr.getPhysicalPlan().getContainers()) {
        result += c.getAllocatedMemoryMB();
      }
      return result;
    }

    @AutoMetric
    @Override
    public long getMemoryRequired()
    {
      long result = 0;
      for (PTContainer c : dnmgr.getPhysicalPlan().getContainers()) {
        if (c.getExternalId() == null || c.getState() == PTContainer.State.KILLED) {
          result += c.getRequiredMemoryMB();
        }
      }
      return result;
    }

    @AutoMetric
    @Override
    public int getTotalVCoresAllocated()
    {
      int result = 0;
      for (PTContainer c : dnmgr.getPhysicalPlan().getContainers()) {
        result += c.getAllocatedVCores();
      }
      return result;
    }

    @AutoMetric
    @Override
    public int getVCoresRequired()
    {
      int result = 0;
      for (PTContainer c : dnmgr.getPhysicalPlan().getContainers()) {
        if (c.getExternalId() == null || c.getState() == PTContainer.State.KILLED) {
          if (c.getRequiredVCores() == 0) {
            result++;
          } else {
            result += c.getRequiredVCores();
          }
        }
      }
      return result;
    }

    @AutoMetric
    @Override
    public long getTotalBufferServerReadBytesPSMA()
    {
      long result = 0;
      for (Map.Entry<Integer, PTOperator> entry : dnmgr.getPhysicalPlan().getAllOperators().entrySet()) {
        for (Map.Entry<String, PortStatus> portEntry : entry.getValue().stats.inputPortStatusList.entrySet()) {
          result += portEntry.getValue().bufferServerBytesPMSMA.getAvg() * 1000;
        }
      }
      return result;
    }

    @AutoMetric
    @Override
    public long getTotalBufferServerWriteBytesPSMA()
    {
      long result = 0;
      for (Map.Entry<Integer, PTOperator> entry : dnmgr.getPhysicalPlan().getAllOperators().entrySet()) {
        for (Map.Entry<String, PortStatus> portEntry : entry.getValue().stats.outputPortStatusList.entrySet()) {
          result += portEntry.getValue().bufferServerBytesPMSMA.getAvg() * 1000;
        }
      }
      return result;
    }

    @Override
    public List<Integer> getCriticalPath()
    {
      StreamingContainerManager.CriticalPathInfo criticalPathInfo = dnmgr.getCriticalPathInfo();
      return (criticalPathInfo == null) ? null : criticalPathInfo.getPath();
    }

    @AutoMetric
    @Override
    public long getLatency()
    {
      StreamingContainerManager.CriticalPathInfo criticalPathInfo = dnmgr.getCriticalPathInfo();
      return (criticalPathInfo == null) ? 0 : criticalPathInfo.getLatency();
    }

    @Override
    public long getWindowStartMillis()
    {
      return dnmgr.getWindowStartMillis();
    }

  }

  public static class ClusterAppContextImpl extends BaseContext implements StramAppContext
  {
    private long startTime;
    private transient ClusterAppStats stats;
    // Application Attempt Id ( combination of attemptId and fail count )
    private transient IApplicationAttemptId appAttemptID;
    // Tracking url to which app master publishes info for clients to monitor
    private String appMasterTrackingUrl = "";

    public ClusterAppContextImpl(Attribute.AttributeMap attributes, long startTime, ClusterAppStats stats,
        IApplicationAttemptId appAttemptID)
    {
      super(attributes, null);
      this.startTime = startTime;
      this.stats = stats;
      this.appAttemptID = appAttemptID;
    }

    public void setAppMasterTrackingUrl(String hostname, int port, Configuration config)
    {
      appMasterTrackingUrl = hostname + ":" + port;
      if (ConfigUtils.isSSLEnabled(config)) {
        appMasterTrackingUrl = "https://" + appMasterTrackingUrl;
      }
      LOG.info("Setting tracking URL to: " + appMasterTrackingUrl);
    }

    public int getApplicationId()
    {
      return appAttemptID.getApplicationId();
    }

    @Override
    public String getApplicationID()
    {
      return appAttemptID.getApplicationID();
    }

    @Override
    public int getApplicationAttemptId()
    {
      return appAttemptID.getApplicationAttemptId();
    }

    @Override
    public String getApplicationName()
    {
      return getValue(LogicalPlan.APPLICATION_NAME);
    }

    @Override
    public String getApplicationDocLink()
    {
      return getValue(LogicalPlan.APPLICATION_DOC_LINK);
    }

    @Override
    public long getStartTime()
    {
      return startTime;
    }

    @Override
    public long getElapsedTime()
    {
      return elapsed(startTime, 0);
    }

    @Override
    public String getApplicationPath()
    {
      return getValue(LogicalPlan.APPLICATION_PATH);
    }

    @Override
    public CharSequence getUser()
    {
      return ClusterProviderFactory.getProvider().getConfiguration().get(Settings.USER);
    }

    @Override
    public String getAppMasterTrackingUrl()
    {
      return appMasterTrackingUrl;
    }

    @Override
    public ClusterAppStats getStats()
    {
      return stats;
    }

    @Override
    public String getGatewayAddress()
    {
      return getValue(LogicalPlan.GATEWAY_CONNECT_ADDRESS);
    }

    @Override
    public boolean isGatewayConnected()
    {
      if (stats.fetchDnmgr() != null) {
        return stats.fetchDnmgr().isGatewayConnected();
      }
      return false;
    }

    @Override
    public List<AppDataSource> getAppDataSources()
    {
      if (stats.fetchDnmgr() != null) {
        return stats.fetchDnmgr().getAppDataSources();
      }
      return null;
    }

    @Override
    public Map<String, Object> getMetrics()
    {
      if (stats.fetchDnmgr() != null) {
        return (Map)stats.fetchDnmgr().getLatestLogicalMetrics();
      }
      return null;
    }

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201309112304L;
  }

  /**
   * Dump out contents of $CWD and the environment to stdout for debugging
   */
  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static void dumpOutDebugInfo(Configuration config)
  {
    LOG.info("Dump debug output");
    Map<String, String> envs = System.getenv();
    LOG.info("\nDumping System Env: begin");
    for (Map.Entry<String, String> env : envs.entrySet()) {
      LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
    }
    LOG.info("Dumping System Env: end");

    String cmd = "ls -al";
    Runtime run = Runtime.getRuntime();
    Process pr;
    try {
      pr = run.exec(cmd);
      pr.waitFor();

      BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
      String line;
      LOG.info("\nDumping files in local dir: begin");
      try {
        while ((line = buf.readLine()) != null) {
          LOG.info("System CWD content: " + line);
        }
        LOG.info("Dumping files in local dir: end");
      } finally {
        buf.close();
      }
    } catch (IOException e) {
      LOG.debug("Exception", e);
    } catch (InterruptedException e) {
      LOG.info("Interrupted", e);
    }

    LOG.info("Classpath: {}", System.getProperty("java.class.path"));
    LOG.info("Config resources: {}", config.toString());
    try {
      // find a better way of logging this using the logger.
      Configuration.dumpConfiguration(config, new PrintWriter(System.out));
    } catch (Exception e) {
      LOG.error("Error dumping configuration.", e);
    }
  }

  public static long elapsed(long started, long finished)
  {
    return elapsed(started, finished, true);
  }

  // A valid elapsed is supposed to be non-negative. If finished/current time
  // is ahead of the started time, return -1 to indicate invalid elapsed time,
  // and record a warning log.
  public static long elapsed(long started, long finished, boolean isRunning)
  {
    if (finished > 0 && started > 0) {
      long elapsed = finished - started;
      if (elapsed >= 0) {
        return elapsed;
      } else {
        LOG.warn("Finished time " + finished
            + " is ahead of started time " + started);
        return -1;
      }
    }
    if (isRunning) {
      long current = System.currentTimeMillis();
      long elapsed = started > 0 ? current - started : 0;
      if (elapsed >= 0) {
        return elapsed;
      } else {
        LOG.warn("Current time " + current
            + " is ahead of started time " + started);
        return -1;
      }
    } else {
      return -1;
    }
  }
}
