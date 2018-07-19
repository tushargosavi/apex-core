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
package com.datatorrent.stram.engine;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.operator.ControlTuple;

import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.common.util.ScheduledExecutorService;
import com.datatorrent.netlet.util.CircularBuffer;
import com.datatorrent.stram.tuple.EndWindowTuple;
import com.datatorrent.stram.tuple.Tuple;

public class WindowGenerator extends MuxReservoir implements Stream
{
  private final ScheduledExecutorService ses;
  private final BlockingQueue<Tuple> queue;
  private int windowWidthMillis; // Window size
  private long windowId;
  private long resetWindowMillis;
  private int checkpointCount = 60; /* default checkpointing after 60 windows */


  public WindowGenerator(ScheduledExecutorService service, int capacity)
  {
    ses = service;
    queue = new CircularBuffer<>(capacity);
  }

  /**
   * Increments window by 1
   */
  public final void advanceWindow()
  {
    windowId++;
  }

  /**
   * Updates window in a circular buffer on inputAdapters<p>
   * This code generates the windows
   */
  private void endCurrentBeginNewWindow() throws InterruptedException
  {
    queue.put(new EndWindowTuple(windowId));
    if (windowId != 0 && (windowId % checkpointCount == 0)) {
      queue.put(new Tuple(MessageType.CHECKPOINT, windowId));
    }
    advanceWindow();
    queue.put(new Tuple(MessageType.BEGIN_WINDOW, windowId));
  }

  public void setResetWindow(long millis)
  {
    resetWindowMillis = millis;
  }

  public void setWindowWidth(int millis)
  {
    windowWidthMillis = millis;
  }

  public void setCheckpointCount(int streamingWindowCount)
  {
    checkpointCount = streamingWindowCount;
  }

  @Override
  public void setup(StreamContext context)
  {
    logger.info("WindowGenerator::setup does not do anything useful, please use setFirstWindow/setResetWindow/setWindowWidth do set properties.");
  }

  private long getWindowTime(long windowId)
  {
    return resetWindowMillis + (windowId * windowWidthMillis);
  }

  private long getWindowId(long ts)
  {
    return (ts - resetWindowMillis) % windowWidthMillis;
  }

  @Override
  public void activate(StreamContext context)
  {
    Runnable subsequentRun = () -> {
      try {
        endCurrentBeginNewWindow();
      } catch (InterruptedException ie) {
        handleException(ie);
      }
    };

    long currentTms = ses.getCurrentTimeMillis();
    long delay = getWindowTime(windowId) - currentTms;
    long targetWindowId = getWindowId(currentTms);
    if (windowId < targetWindowId) {
      logger.info("Catching up from {} to {}", windowId, targetWindowId);
      ses.schedule(
          () -> {
            try {
              sendBeginWindow();
              do {
                endCurrentBeginNewWindow();
              } while (windowId < targetWindowId);
            } catch (InterruptedException ie) {
              handleException(ie);
            }
          },
          0, TimeUnit.MILLISECONDS);
    } else {
      logger.info("The input will start to be sliced in {} milliseconds", delay);
      ses.schedule(() -> {
        try {
          sendBeginWindow();
        } catch (InterruptedException e) {
          handleException(e);
        }
      }, delay, TimeUnit.MILLISECONDS);
    }

    ses.scheduleAtFixedRate(subsequentRun, delay + windowWidthMillis, windowWidthMillis, TimeUnit.MILLISECONDS);
  }


  public void setFirstWindow(long w)
  {
    this.windowId = w;
  }

  private void sendBeginWindow() throws InterruptedException
  {
    queue.put(new Tuple(MessageType.BEGIN_WINDOW, windowId));
  }

  public static long compareWindowId(long windowIdA, long windowIdB, long windowWidthMillis)
  {
    return windowIdB - windowIdA;
  }

  @Override
  public void deactivate()
  {
    ses.shutdown();
  }

  private void handleException(Exception e)
  {
    if (e instanceof InterruptedException) {
      ses.shutdown();
    } else {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void put(Object tuple)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean putControl(ControlTuple payload)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  protected Queue getQueue()
  {
    return queue;
  }

  @Override
  public int getCount(boolean reset)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public static long getWindowCount(long millis, long firstMillis, long widthMillis)
  {
    return (millis - firstMillis) / widthMillis;
  }

  public static long getWindowId(long millis, long firstWindowMillis, long windowWidthMillis)
  {
    return (millis - firstWindowMillis) / windowWidthMillis;
  }

  /**
   * @param windowId
   * @param firstWindowMillis
   * @param windowWidthMillis
   * @return the milliseconds for next window.
   */
  public static long getNextWindowMillis(long windowId, long firstWindowMillis, long windowWidthMillis)
  {
    return getWindowMillis(windowId, firstWindowMillis, windowWidthMillis) + windowWidthMillis;
  }

  public static long getNextWindowId(long windowId, long firstWindowMillis, long windowWidthMillis)
  {
    return getAheadWindowId(windowId, firstWindowMillis, windowWidthMillis, 1);
  }

  public static long getAheadWindowId(long windowId, long firstWindowMillis, long windowWidthMillis, int ahead)
  {
    long millis = getWindowMillis(windowId, firstWindowMillis, windowWidthMillis);
    millis += ahead * windowWidthMillis;
    return getWindowId(millis, firstWindowMillis, windowWidthMillis);
  }

  /**
   * @param windowId
   * @param firstWindowMillis
   * @param windowWidthMillis
   * @return the milliseconds for given window.
   */
  public static long getWindowMillis(long windowId, long firstWindowMillis, long windowWidthMillis)
  {
    if (windowId == -1) {
      return firstWindowMillis;
    }
    return firstWindowMillis + (windowId * windowWidthMillis);
  }

  private static final Logger logger = LoggerFactory.getLogger(WindowGenerator.class);
}
