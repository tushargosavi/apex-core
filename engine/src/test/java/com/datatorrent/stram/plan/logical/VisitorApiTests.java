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

import javax.validation.ValidationException;

import org.junit.Test;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

public class VisitorApiTests
{

  public interface Validator
  {
    void validate()  throws ValidationException;
  }

  public static class OperatorValidator implements DAG.DAGVisitor
  {
    @Override
    public void startDAG(DAG dag)
    {

    }

    @Override
    public void visitOperator(DAG.OperatorMeta ometa)
    {
      Operator operator = ometa.getOperator();
      if (operator instanceof Validator) {
        ((Validator)operator).validate();
      }
    }

    @Override
    public void visitStream(DAG.StreamMeta smeta)
    {

    }

    @Override
    public void endDAG()
    {

    }
  }

  static class Input extends BaseOperator implements InputOperator, Validator
  {

    public transient DefaultOutputPort<Integer> out = new DefaultOutputPort<>();

    @Override
    public void emitTuples()
    {
      out.emit(1);
      out.emit(2);
      throw new ShutdownException();
    }

    @Override
    public void validate() throws ValidationException
    {
      if (!out.isConnected()) {
        throw new ValidationException("Output port needs to be connected");
      }
    }
  }

  static class Operator1  extends BaseOperator implements Validator
  {

    public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
    {
      @Override
      public void process(Integer tuple)
      {

      }
    };

    @Override
    public void validate() throws ValidationException
    {

    }
  }

  @Test
  public void testOperatorValidator()
  {
    LogicalPlan dag = new LogicalPlan();
    Input input = dag.addOperator("input", new Input());
    Operator1 operator = dag.addOperator("operator2", new Operator1());
    dag.addStream("Stream1", input.out, operator.input);
    dag.addVisitor(new OperatorValidator());
    dag.validate();
  }

  @Test
  public void testOperatorValidator1()
  {
    LogicalPlan dag = new LogicalPlan();
    Input input = dag.addOperator("input", new Input());
    dag.addVisitor(new OperatorValidator());
    dag.validate();
  }
}
