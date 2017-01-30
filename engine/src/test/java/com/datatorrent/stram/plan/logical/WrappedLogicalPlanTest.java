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

import org.junit.Test;

import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.InputNodeTest;

/**
 * Tests for modifying logical plan
 */
public class WrappedLogicalPlanTest
{
  @Test
  public void testAddOperator()
  {
    LogicalPlan plan = new LogicalPlan();
    InputNodeTest.TestInputOperator input = plan.addOperator("input", new InputNodeTest.TestInputOperator());
    plan.validate();

    WrappedLogicalPlan cdag = plan.startTransaction();
    GenericTestOperator sinkOperator = cdag.addOperator("input1", new GenericTestOperator());
    cdag.addStream("s1", input.output, sinkOperator.inport1);
    cdag.commit();
  }
}