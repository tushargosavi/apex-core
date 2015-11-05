/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.plan.logical;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.engine.GenericOperatorProperty;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class TestModules
{

  public static class GenericModule implements Module
  {
    private static final Logger LOG = LoggerFactory.getLogger(TestModules.class);

    public volatile Object inport1Tuple = null;

    @OutputPortFieldAnnotation(optional = true)
    final public transient DefaultOutputPort<Object> outport1 = new DefaultOutputPort<Object>();

    @OutputPortFieldAnnotation(optional = true)
    final public transient DefaultOutputPort<Object> outport2 = new DefaultOutputPort<Object>();

    private String emitFormat;

    public boolean booleanProperty;

    private String myStringProperty;

    private transient GenericOperatorProperty genericOperatorProperty = new GenericOperatorProperty("test");

    public String getMyStringProperty()
    {
      return myStringProperty;
    }

    public void setMyStringProperty(String myStringProperty)
    {
      this.myStringProperty = myStringProperty;
    }

    public boolean isBooleanProperty()
    {
      return booleanProperty;
    }

    public void setBooleanProperty(boolean booleanProperty)
    {
      this.booleanProperty = booleanProperty;
    }

    public String propertySetterOnly;

    /**
     * setter w/o getter defined
     *
     * @param v
     */
    public void setStringPropertySetterOnly(String v)
    {
      this.propertySetterOnly = v;
    }

    public String getEmitFormat()
    {
      return emitFormat;
    }

    public void setEmitFormat(String emitFormat)
    {
      this.emitFormat = emitFormat;
    }

    public GenericOperatorProperty getGenericOperatorProperty()
    {
      return genericOperatorProperty;
    }

    public void setGenericOperatorProperty(GenericOperatorProperty genericOperatorProperty)
    {
      this.genericOperatorProperty = genericOperatorProperty;
    }

    private void processInternal(Object o)
    {
      LOG.debug("Got some work: " + o);
      if (emitFormat != null) {
        o = String.format(emitFormat, o);
      }
      if (outport1.isConnected()) {
        outport1.emit(o);
      }
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      LOG.info("populateDAG of module called");
    }
  }

  public static class RandGen extends BaseOperator implements InputOperator
  {
    private int min = 0;
    private int max = 100;
    public transient DefaultOutputPort<Integer> out = new DefaultOutputPort<>();
    private transient Random rand = new Random();
    private int tupleBlast;
    private transient int count;

    @Override
    public void emitTuples()
    {
      for (; count < tupleBlast; count++) {
        out.emit(rand.nextInt(max));
      }
    }

    @Override
    public void beginWindow(long windowId)
    {
      count = 0;
    }

    public int getMin()
    {
      return min;
    }

    public void setMin(int min)
    {
      this.min = min;
    }

    public int getMax()
    {
      return max;
    }

    public void setMax(int max)
    {
      this.max = max;
    }

    public int getTupleBlast()
    {
      return tupleBlast;
    }

    public void setTupleBlast(int tupleBlast)
    {
      this.tupleBlast = tupleBlast;
    }
  }

  public static class PiCalculator extends BaseOperator
  {
    private int size;

    public transient DefaultInputPort<Integer> in = new DefaultInputPort<Integer>()
    {
      @Override
      public void process(Integer tuple)
      {
        //LOG.debug("processing tuple ", tuple);
        out.emit(tuple);
      }
    };

    public transient DefaultOutputPort<Integer> out = new DefaultOutputPort<>();

    public int getSize()
    {
      return size;
    }

    public void setSize(int size)
    {
      this.size = size;
    }
  }

  public static class PiModule implements Module
  {

    private int size;

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      RandGen gen = dag.addOperator("gen", new RandGen());
      gen.setMax(size);
      PiCalculator pc = dag.addOperator("cal", new PiCalculator());
      pc.setSize(size);
      dag.addStream("s1", gen.out, pc.in);
    }

    public int getSize()
    {
      return size;
    }

    public void setSize(int size)
    {
      this.size = size;
    }
  }

  public static class WrapperModule implements Module
  {
    private int size;

    @InputPortFieldAnnotation(optional = true)
    public transient DefaultInputPort in = new DefaultInputPort<Integer>()
    {
      @Override
      public void process(Integer tuple)
      {

      }
    };

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      PiModule pi = dag.addModule("PiModule", PiModule.class);
      pi.setSize(size);
    }

    public void setSize(int size)
    {
      this.size = size;
    }

    public int getSize()
    {
      return size;
    }
  }

  public static class RandGenModule implements Module
  {
    @OutputPortFieldAnnotation(optional = true)
    public transient DefaultOutputPort<Integer> out = new DefaultOutputPort<>();

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      RandGen rand = dag.addOperator("RandGen", RandGen.class);
    }
  }
}
