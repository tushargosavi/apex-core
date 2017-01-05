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

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.service.CompositeService;

import com.google.common.collect.Lists;

import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;

/**
 * A top level ApexServiceProcessor which will handle multiple requests through
 * a thread pool.
 * TODO: allow user to configure the thread-pool size.
 */
public class ApexServiceProcessor extends CompositeService implements ApexService
{
  private static final Logger LOG = LoggerFactory.getLogger(ApexServiceProcessor.class);
  private Collection<ApexService> userServices = Lists.newArrayList();
  private ExecutorService poolExecutor;
  private final StramAppContext appContext;
  private final StreamingContainerManager dmgr;

  public ApexServiceProcessor(StramAppContext context, StreamingContainerManager dmgr)
  {
    super(ApexServiceProcessor.class.getName());
    this.appContext = context;
    this.dmgr = dmgr;
    LOG.info("Creating appex service ");
  }

  public void addUserService(ApexService service)
  {
    service.setAppContext(appContext);
    service.setDagManager(dmgr);
    userServices.add(service);
    LOG.info("Adding user service {}", service.getName());
    addService(service);
  }

  @Override
  public void setAppContext(StramAppContext context)
  {

  }

  @Override
  public void setDagManager(StreamingContainerManager manager)
  {

  }

  @Override
  public void handleHeartbeat(ContainerHeartbeat hb)
  {
    poolExecutor.submit(new HeartbeatDeliveryTask(hb));
  }

  @Override
  public void tick()
  {
    poolExecutor.submit(new TickTask());
  }

  @Override
  public void handleEvent(StramEvent event)
  {

  }

  @Override
  protected void serviceStart() throws Exception
  {
    super.serviceStart();
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

  private class HeartbeatDeliveryTask implements Runnable
  {
    private final ContainerHeartbeat hb;

    public HeartbeatDeliveryTask(ContainerHeartbeat hb)
    {
      this.hb = hb;
    }

    @Override
    public void run()
    {
      for (ApexService service : userServices) {
        service.handleHeartbeat(hb);
      }
    }
  }

  private class TickTask implements Runnable
  {
    @Override
    public void run()
    {
      for (ApexService service : userServices) {
        service.tick();
      }
    }
  }
}
