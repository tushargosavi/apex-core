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
package com.datatorrent.stram.plan.logical.module;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;

/**
 * Outer module - Level 1
 * Has an Operator - FilterOperator and a Module - OddEvenModule (Level 2)
 */
public class OuterModule implements Module
{

  public transient ProxyInputPort<Integer> mInput = new ProxyInputPort<Integer>();
  public transient ProxyOutputPort<Integer> mOutputEven = new ProxyOutputPort<Integer>();
  public transient ProxyOutputPort<Integer> mOutputOdd = new ProxyOutputPort<Integer>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FilterOperator filter = dag.addOperator("FilterOperator", new FilterOperator());
    mInput.set(filter.input);

    InnerModule innerModule = dag.addModule("InnerModule", new InnerModule());
    mOutputEven.set(innerModule.mOutputEven);
    mOutputOdd.set(innerModule.mOutputOdd);

    dag.addStream("FilterToInnerModule", filter.output, innerModule.mInput);
  }
}
