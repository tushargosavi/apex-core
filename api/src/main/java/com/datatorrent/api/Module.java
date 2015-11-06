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
package com.datatorrent.api;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Operator.Port;
import com.datatorrent.api.Operator.Unifier;

@InterfaceStability.Evolving
public interface Module
{
  void populateDAG(DAG dag, Configuration conf);

  public interface ProxyPort<T> extends Port
  {
    void set(T port);

    T get();
  }

  public final class ProxyInputPort<T> implements ProxyPort<InputPort<T>>, InputPort<T>
  {
    InputPort<T> inputPort;

    @Override
    public void set(InputPort<T> port)
    {
      inputPort = port;
    }

    @Override
    public InputPort<T> get()
    {
      return inputPort;
    }

    @Override
    public void setup(PortContext context)
    {
      if (inputPort != null) {
        inputPort.setup(context);
      }
    }

    @Override
    public void teardown()
    {
      if (inputPort != null) {
        inputPort.teardown();
      }
    }

    @Override
    public Sink<T> getSink()
    {
      if (inputPort != null) {
        return inputPort.getSink();
      } else {
        return null;
      }
    }

    @Override
    public void setConnected(boolean connected)
    {
      if (inputPort != null) {
        inputPort.setConnected(connected);
      }
    }

    @Override
    public StreamCodec<T> getStreamCodec()
    {
      if (inputPort != null) {
        return inputPort.getStreamCodec();
      } else {
        return null;
      }
    }
  }

  public final class ProxyOutputPort<T> implements ProxyPort<OutputPort<T>>, OutputPort<T>
  {
    OutputPort<T> outputPort;

    public void set(OutputPort<T> port)
    {
      outputPort = port;
    }

    public OutputPort<T> get()
    {
      return outputPort;
    }

    @Override
    public void setup(PortContext context)
    {
      if (outputPort != null) {
        outputPort.setup(context);
      }
    }

    @Override
    public void teardown()
    {
      if (outputPort != null) {
        outputPort.teardown();
      }
    }

    @Override
    public void setSink(Sink<Object> s)
    {
      if (outputPort != null) {
        outputPort.setSink(s);
      }
    }

    @Override
    public Unifier<T> getUnifier()
    {
      if (outputPort != null) {
        return outputPort.getUnifier();
      } else {
        return null;
      }
    }
  }
}

