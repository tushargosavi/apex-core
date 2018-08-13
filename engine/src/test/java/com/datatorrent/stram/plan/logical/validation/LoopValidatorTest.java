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
package com.datatorrent.stram.plan.logical.validation;

import java.util.Collections;
import java.util.Set;

import javax.validation.ValidationException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import com.datatorrent.common.util.DefaultDelayOperator;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.validators.InvalidLoopValidator;
import com.datatorrent.stram.plan.logical.visitor.CycleDetector;
import com.datatorrent.stram.plan.logical.visitor.CycleDetector.ValidationContext;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

public class LoopValidatorTest extends LogicalPlanTestBase
{

  private static final Logger LOG = LoggerFactory.getLogger(LoopValidatorTest.class);

  @Test
  public void testCycleDetection()
  {
    InvalidLoopValidator validator = new InvalidLoopValidator();

    //NodeConf operator1 = b.getOrAddNode("operator1");
    GenericTestOperator operator2 = dag.addOperator("operator2", GenericTestOperator.class);
    GenericTestOperator operator3 = dag.addOperator("operator3", GenericTestOperator.class);
    GenericTestOperator operator4 = dag.addOperator("operator4", GenericTestOperator.class);
    //NodeConf operator5 = b.getOrAddNode("operator5");
    //NodeConf operator6 = b.getOrAddNode("operator6");
    GenericTestOperator operator7 = dag.addOperator("operator7", GenericTestOperator.class);

    // strongly connect n2-n3-n4-n2
    dag.addStream("n2n3", operator2.outport1, operator3.inport1);
    dag.addStream("n3n4", operator3.outport1, operator4.inport1);
    dag.addStream("n4n2", operator4.outport1, operator2.inport1);

    // self referencing operator cycle
    LogicalPlan.StreamMeta n7n7 = dag.addStream("n7n7", operator7.outport1, operator7.inport1);

    // This should be part of another test
    try {
      n7n7.addSink(operator7.inport1);
      fail("cannot add to stream again");
    } catch (Exception e) {
      // expected, stream can have single input/output only
    }

    ValidationContext vc = new ValidationContext();
    CycleDetector detector = new CycleDetector();
    detector.findStronglyConnected(dag.getMeta(operator7), vc);
    assertEquals("operator self reference", 1, vc.invalidCycles.size());
    assertEquals("operator self reference", 1, vc.invalidCycles.get(0).size());
    assertEquals("operator self reference", dag.getMeta(operator7), vc.invalidCycles.get(0).iterator().next());

    // 3 operator cycle
    vc = new ValidationContext();
    detector.findStronglyConnected(dag.getMeta(operator4), vc);
    assertEquals("3 operator cycle", 1, vc.invalidCycles.size());
    assertEquals("3 operator cycle", 3, vc.invalidCycles.get(0).size());
    assertTrue("operator2", vc.invalidCycles.get(0).contains(dag.getMeta(operator2)));
    assertTrue("operator3", vc.invalidCycles.get(0).contains(dag.getMeta(operator3)));
    assertTrue("operator4", vc.invalidCycles.get(0).contains(dag.getMeta(operator4)));

    try {
      dag.validate();
      fail("validation should fail");
    } catch (ValidationException e) {
      LOG.info("Expected validation exception ", e);
    }
  }


  @Test
  public void testCycleDetectionWithDelay()
  {
    TestGeneratorInputOperator opA = dag.addOperator("A", TestGeneratorInputOperator.class);
    GenericTestOperator opB = dag.addOperator("B", GenericTestOperator.class);
    GenericTestOperator opC = dag.addOperator("C", GenericTestOperator.class);
    GenericTestOperator opD = dag.addOperator("D", GenericTestOperator.class);
    DefaultDelayOperator<Object> opDelay = dag.addOperator("opDelay", new DefaultDelayOperator<>());
    DefaultDelayOperator<Object> opDelay2 = dag.addOperator("opDelay2", new DefaultDelayOperator<>());

    dag.addStream("AtoB", opA.outport, opB.inport1);
    dag.addStream("BtoC", opB.outport1, opC.inport1);
    dag.addStream("CtoD", opC.outport1, opD.inport1);
    dag.addStream("CtoDelay", opC.outport2, opDelay.input);
    dag.addStream("DtoDelay", opD.outport1, opDelay2.input);
    dag.addStream("DelayToB", opDelay.output, opB.inport2);
    dag.addStream("Delay2ToC", opDelay2.output, opC.inport2);

    CycleDetector.ValidationContext vc = new CycleDetector.ValidationContext();
    CycleDetector detector = new CycleDetector();
    detector.findStronglyConnected(dag.getMeta(opA), vc);

    Assert.assertEquals("No invalid cycle", Collections.emptyList(), vc.invalidCycles);
    Set<LogicalPlan.OperatorMeta> exp = Sets.newHashSet(dag.getMeta(opDelay2), dag.getMeta(opDelay), dag.getMeta(opC), dag.getMeta(opB), dag.getMeta(opD));
    Assert.assertEquals("cycle", exp, vc.stronglyConnected.get(0));
  }

}
