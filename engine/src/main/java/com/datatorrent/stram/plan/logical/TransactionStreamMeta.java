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
 * This is a new streamMeta. But the port connections can be from the old DAG.
 * The more extensive validation will be performed later, this will only be
 * used to add a new information.
 */
public class TransactionStreamMeta implements DAG.StreamMeta
{
  private final DAGChangeTransactionImpl transaction;
  private final String name;
  private Operator.OutputPort<?> source;
  private List<Operator.InputPort<?>> sinks = new ArrayList<>();
  private List<LogicalPlan.InputPortMeta> sinksMeta = new ArrayList<>();
  private DAG.Locality locality;

  public TransactionStreamMeta(String name, DAGChangeTransactionImpl transaction)
  {
    this.name = name;
    this.transaction = transaction;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public DAG.Locality getLocality()
  {
    return locality;
  }

  @Override
  public DAG.StreamMeta setLocality(DAG.Locality locality)
  {
    this.locality = locality;
    return this;
  }

  @Override
  public DAG.StreamMeta setSource(Operator.OutputPort<?> port)
  {
    source = port;
    return this;
  }


  @Override
  public DAG.StreamMeta addSink(Operator.InputPort<?> port)
  {
    sinks.add(port);
    sinksMeta.add(transaction.getPortMeta(port));
    return this;
  }

  @Override
  public DAG.StreamMeta persistUsing(String name, Operator persistOperator, Operator.InputPort<?> persistOperatorInputPort)
  {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DAG.StreamMeta persistUsing(String name, Operator persistOperator)
  {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DAG.StreamMeta persistUsing(String name, Operator persistOperator, Operator.InputPort<?> persistOperatorInputPort, Operator.InputPort<?> sinkToPersist)
  {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public <T extends DAG.OutputPortMeta> T getSource()
  {
    return (T)transaction.getPortMeta(source);
  }

  @Override
  public <T extends DAG.InputPortMeta> Collection<T> getSinks()
  {
    return (Collection<T>)sinksMeta;
  }
}
