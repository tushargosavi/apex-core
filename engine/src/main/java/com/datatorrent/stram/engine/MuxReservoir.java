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

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.datatorrent.api.Sink;
import com.datatorrent.netlet.util.CircularBuffer;
import com.datatorrent.stram.tuple.Tuple;

/**
 * <p>Abstract MuxReservoir class.</p>
 *
 * @since 0.3.2
 */
public abstract class MuxReservoir
{
  @SuppressWarnings("VolatileArrayField")
  private ConcurrentMap<String, SubReservoir> reservoirMap = new ConcurrentHashMap<>();

  public SweepableReservoir acquireReservoir(String id, int capacity)
  {
    SubReservoir r = reservoirMap.get(id);
    if (r == null) {
      r = new SubReservoir(capacity);
      reservoirMap.putIfAbsent(id, r);
    }
    return r;
  }

  public SweepableReservoir releaseReservoir(String id)
  {
    return reservoirMap.remove(id);
  }

  protected abstract Queue getQueue();

  class SubReservoir extends CircularBuffer<Object> implements SweepableReservoir
  {
    int count;
    private Sink<Object> sink;

    SubReservoir(int capacity)
    {
      super(capacity);
    }

    @Override
    public int size(final boolean dataTupleAware)
    {
      int size = size();
      if (dataTupleAware) {
        Iterator<Object> iterator = getFrozenIterator();
        while (iterator.hasNext()) {
          if (iterator.next() instanceof Tuple) {
            size--;
          }
        }
      }
      return size;
    }

    @Override
    public Sink<Object> setSink(Sink<Object> sink)
    {
      try {
        return this.sink;
      } finally {
        this.sink = sink;
      }
    }

    @Override
    public Tuple sweep()
    {
      final int size = size();
      if (size > 0) {
        for (int i = 0; i < size; i++) {
          if (peekUnsafe() instanceof Tuple) {
            count += i;
            return (Tuple)peekUnsafe();
          }
          sink.put(pollUnsafe());
        }

        count += size;
      }

      final Queue queue = getQueue();
      synchronized (queue) {
        if (queue.isEmpty()) {
          return null;
        }

        /* find out the minimum remaining capacity in all the other buffers and consume those many tuples from bufferserver */
        int min = Integer.MAX_VALUE;
        for (SubReservoir r : reservoirMap.values()) {
          if (r.remainingCapacity() < min) {
            min = r.remainingCapacity();
          }
        }

        while (min-- > 0) {
          Object o = queue.poll();
          if (o == null) {
            break;
          }
          for (SubReservoir r  : reservoirMap.values()) {
            r.add(o);
          }
        }
      }

      return null;
    }

    @Override
    public int getCount(boolean reset)
    {
      try {
        return count;
      } finally {
        if (reset) {
          count = 0;
        }
      }
    }
  }
}
