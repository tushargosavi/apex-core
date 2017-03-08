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
package com.datatorrent.stram.plan.logical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;

/**
 * During DAG change transaction, all existing streams are wrapped by this class.
 * This class only allows adding a new sink to it. i.e user can extend the original
 * stream.
 */
public class ExtendableStreamMeta implements DAG.StreamMeta
{

  /* the original stream meta */
  final DAG.StreamMeta parent;
  private List<Operator.InputPort<?>> newSinks = new ArrayList<>();

  public ExtendableStreamMeta(DAG.StreamMeta parent)
  {
    this.parent = parent;
  }

  @Override
  public String getName()
  {
    return parent.getName();
  }

  @Override
  public DAG.Locality getLocality()
  {
    return parent.getLocality();
  }

  @Override
  public DAG.StreamMeta setLocality(DAG.Locality locality)
  {
    throw new UnsupportedOperationException("Can not set locality of existing stream");
  }

  @Override
  public DAG.StreamMeta setSource(Operator.OutputPort<?> port)
  {
    throw new UnsupportedOperationException("Can not set source of existing stream");
  }

  @Override
  public DAG.StreamMeta addSink(Operator.InputPort<?> port)
  {
    newSinks.add(port);
    return this;
  }

  @Override
  public DAG.StreamMeta persistUsing(String name, Operator persistOperator, Operator.InputPort<?> persistOperatorInputPort)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public DAG.StreamMeta persistUsing(String name, Operator persistOperator)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public DAG.StreamMeta persistUsing(String name, Operator persistOperator, Operator.InputPort<?> persistOperatorInputPort, Operator.InputPort<?> sinkToPersist)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends DAG.OutputPortMeta> T getSource()
  {
    return (T)parent.getSource();
  }

  @Override
  public <T extends DAG.InputPortMeta> Collection<T> getSinks()
  {
    // return null;
    throw new RuntimeException("not implemented");
  }

  public <T extends DAG.InputPortMeta> Collection<Operator.InputPort<?>> getNewSinks()
  {
    return newSinks;
  }
}
