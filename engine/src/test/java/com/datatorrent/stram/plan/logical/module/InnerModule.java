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
package com.datatorrent.stram.plan.logical.module;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;

/**
 * Inner Module - Level 2
 * Has an Operator which splits the stream into odd and even integers
 */
public class InnerModule implements Module
{
  /*
   * Proxy ports for the module
   * mInput - Input Proxy Port
   * mOutputEven, mOutputOdd - Output Proxy Ports
   */
  public transient ProxyInputPort<Integer> mInput = new ProxyInputPort<Integer>();
  public transient ProxyOutputPort<Integer> mOutputEven = new ProxyOutputPort<Integer>();
  public transient ProxyOutputPort<Integer> mOutputOdd = new ProxyOutputPort<Integer>();

  /**
   * populateDag() method for the module.
   * Called when expanding the logical Dag
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    OddEvenOperator moduleOperator = dag.addOperator("InnerModule", new OddEvenOperator());
    /*
     * Set the operator ports inside the proxy port objects
     */
    mInput.set(moduleOperator.input);
    mOutputEven.set(moduleOperator.even);
    mOutputOdd.set(moduleOperator.odd);
  }
}
