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
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.service.AbstractService;

import com.google.common.collect.Lists;

import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;

public class ApexServiceProcessor<QueueType extends BlockingQueue<? extends ContainerHeartbeat>> extends AbstractService
{
  private static final Logger LOG = LoggerFactory.getLogger(ApexServiceProcessor.class);
  final QueueType queue;
  private Thread thread;
  private volatile boolean stopped = false;
  Collection<ApexService> userServices = Lists.newArrayList();

  public ApexServiceProcessor(QueueType queue)
  {
    super("ApexServiceProcessor");
    this.queue = queue;
  }

  @Override
  protected void serviceStart() throws Exception
  {
    super.serviceStart();
    thread = new QueueProcessorThread();
    thread.start();
  }

  class QueueProcessorThread extends Thread
  {
    @Override
    public void run()
    {
      ContainerHeartbeat heartbeat = null;
      while (!stopped) {
        try {
          heartbeat = queue.take();
        } catch (InterruptedException e) {
          if (stopped) {
            break;
          } else {
            LOG.info("error while taking from queue");
          }
        }
      }

      for (ApexService service : userServices) {
        service.handleHeartbeat(heartbeat);
      }
    }
  }

  @Override
  protected void serviceStop() throws Exception
  {
    super.serviceStop();
    stopped = true;
    thread.interrupt();
    thread.join();
  }
}
