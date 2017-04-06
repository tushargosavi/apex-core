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
import org.slf4j.Logger;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.engine.GenericTestOperator;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Tests for modifying logical plan
 */
public class WrappedLogicalPlanTest
{
  private static final Logger LOG = getLogger(WrappedLogicalPlanTest.class);

  private static class TestInputOperator extends BaseOperator implements InputOperator, Operator.IdleTimeHandler
  {
    public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<>();

    public boolean trueEmitTuplesFalseHandleIdleTime = true;
    private long lastTimestamp;

    @Override
    public void emitTuples()
    {
      if (trueEmitTuplesFalseHandleIdleTime) {
        emit(100L);
      }
    }

    @Override
    public void handleIdleTime()
    {
      if (!trueEmitTuplesFalseHandleIdleTime) {
        emit(100L);
      }
    }

    private void emit(long delay)
    {
      if (System.currentTimeMillis() - lastTimestamp > delay) {
        lastTimestamp = System.currentTimeMillis();
        output.emit(1L);
      }
    }
  }

  @Test
  public void testAddOperatorAndConnectToOld()
  {
    LogicalPlan plan = new LogicalPlan();
    TestInputOperator input = plan.addOperator("input", new TestInputOperator());
    plan.validate();

    DAGChangeTransactionImpl cdag = (DAGChangeTransactionImpl)plan.startTransaction();
    GenericTestOperator sinkOperator = cdag.addOperator("output", new GenericTestOperator());
    TestInputOperator input1 = (TestInputOperator)cdag.getOperatorMeta("input").getOperator();
    cdag.addStream("s1", input.output, sinkOperator.inport1);
    cdag.commit();

    plan.validate();
    LOG.info("number of operators in new dag {}", plan.getAllOperators().size());
    LOG.info("number of streams in new dag {}", plan.getAllStreams().size());
  }

  @Test
  public void testAddOperatorAndConnectToOldStream()
  {
    LogicalPlan plan = new LogicalPlan();
    TestInputOperator input = plan.addOperator("input", new TestInputOperator());
    GenericTestOperator output = plan.addOperator("output", new GenericTestOperator());
    plan.addStream("s1", input.output, output.inport1);
    plan.validate();

    DAGChangeTransactionImpl cdag = (DAGChangeTransactionImpl)plan.startTransaction();
    GenericTestOperator sinkOperator = cdag.addOperator("output1", new GenericTestOperator());
    DAG.StreamMeta smeta = cdag.getStream("s1");
    smeta.addSink(sinkOperator.inport1);
    cdag.commit();

    plan.validate();
    LOG.info("number of operators in new dag {}", plan.getAllOperators().size());
    LOG.info("number of streams in new dag {}", plan.getAllStreams().size());
    smeta = plan.getStream("s1");
    LOG.info("Number of sink in stream is {}", smeta.getSinks().size());
  }

  @Test
  public void testAddOperatorAndConnectToOldOperator()
  {
    LogicalPlan plan = new LogicalPlan();
    TestInputOperator input = plan.addOperator("input", new TestInputOperator());
    GenericTestOperator output = plan.addOperator("output", new GenericTestOperator());
    plan.addStream("s1", input.output, output.inport1);
    plan.validate();

    DAGChangeTransactionImpl cdag = (DAGChangeTransactionImpl)plan.startTransaction();
    TestInputOperator input1 = plan.addOperator("input1", new TestInputOperator());
    DAG.OperatorMeta ometa = cdag.getOperatorMeta("output");
    GenericTestOperator op1 = (GenericTestOperator)ometa.getOperator();
    cdag.addStream("s2", input1.output, op1.inport2);
    cdag.commit();

    plan.validate();
    LOG.info("number of operators in new dag {}", plan.getAllOperators().size());
    LOG.info("number of streams in new dag {}", plan.getAllStreams().size());
  }

  @Test
  public void testAddDisconnectedDAG()
  {
    LogicalPlan plan = new LogicalPlan();
    TestInputOperator input = plan.addOperator("input", new TestInputOperator());
    plan.validate();

    DAGChangeTransactionImpl cdag = (DAGChangeTransactionImpl)plan.startTransaction();
    TestInputOperator input1 = cdag.addOperator("input1", new
        TestInputOperator());
    GenericTestOperator sinkOperator = cdag.addOperator("output1", new GenericTestOperator());
    cdag.addStream("s1", input1.output, sinkOperator.inport1);
    cdag.commit();

    plan.validate();
    LOG.info("number of operators in new dag {}", plan.getAllOperators().size());
    LOG.info("number of streams in new dag {}", plan.getAllStreams().size());
  }

}
