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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Module;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

public class ExtremeModuleTest
{
  public static class DummyInputOperator extends BaseOperator implements InputOperator
  {
    private int inputOperatorProp = 0;

    Random r = new Random();
    public transient DefaultOutputPort<Integer> out = new DefaultOutputPort<Integer>();

    @Override
    public void emitTuples()
    {
      out.emit(r.nextInt());
    }

    public int getInputOperatorProp()
    {
      return inputOperatorProp;
    }

    public void setInputOperatorProp(int inputOperatorProp)
    {
      this.inputOperatorProp = inputOperatorProp;
    }
  }

  public static class DummyOperator extends BaseOperator
  {
    private int operatorProp = 0;

    @OutputPortFieldAnnotation(optional = true)
    public transient final DefaultOutputPort<Integer> out1 = new DefaultOutputPort<>();

    @OutputPortFieldAnnotation(optional = true)
    public transient final DefaultOutputPort<Integer> out2 = new DefaultOutputPort<>();

    @InputPortFieldAnnotation(optional = true)
    public transient final DefaultInputPort<Integer> in = new DefaultInputPort<Integer>()
    {
      @Override
      public void process(Integer tuple)
      {
        out1.emit(tuple);
        out2.emit(tuple);
      }
    };

    public int getOperatorProp()
    {
      return operatorProp;
    }

    public void setOperatorProp(int operatorProp)
    {
      this.operatorProp = operatorProp;
    }
  }

  public static class Level1Module implements Module
  {
    private int level1ModuleProp = 0;

    @InputPortFieldAnnotation(optional = true)
    public transient final ProxyInputPort<Integer> mIn = new ProxyInputPort<>();
    @OutputPortFieldAnnotation(optional = true)
    public transient final ProxyOutputPort<Integer> mOut = new ProxyOutputPort<>();

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      DummyOperator o1 = dag.addOperator("O1", new DummyOperator());
      o1.setOperatorProp(level1ModuleProp);
      mIn.set(o1.in);
      mOut.set(o1.out1);
    }

    public int getLevel1ModuleProp()
    {
      return level1ModuleProp;
    }

    public void setLevel1ModuleProp(int level1ModuleProp)
    {
      this.level1ModuleProp = level1ModuleProp;
    }
  }

  public static class Level2ModuleA implements Module
  {
    private int level2ModuleAProp1 = 0;
    private int level2ModuleAProp2 = 0;
    private int level2ModuleAProp3 = 0;

    @InputPortFieldAnnotation(optional = true)
    public transient final ProxyInputPort<Integer> mIn = new ProxyInputPort<>();

    @OutputPortFieldAnnotation(optional = true)
    public transient final ProxyOutputPort<Integer> mOut1 = new ProxyOutputPort<>();

    @OutputPortFieldAnnotation(optional = true)
    public transient final ProxyOutputPort<Integer> mOut2 = new ProxyOutputPort<>();

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      Level1Module m1 = dag.addModule("M1", new Level1Module());
      m1.setLevel1ModuleProp(level2ModuleAProp1);

      Level1Module m2 = dag.addModule("M2", new Level1Module());
      m2.setLevel1ModuleProp(level2ModuleAProp2);

      DummyOperator o1 = dag.addOperator("O1", new DummyOperator());
      o1.setOperatorProp(level2ModuleAProp3);

      dag.addStream("M1_M2&O1", m1.mOut, m2.mIn, o1.in);

      mIn.set(m1.mIn);
      mOut1.set(m2.mOut);
      mOut2.set(o1.out1);
    }

    public int getLevel2ModuleAProp1()
    {
      return level2ModuleAProp1;
    }

    public void setLevel2ModuleAProp1(int level2ModuleAProp1)
    {
      this.level2ModuleAProp1 = level2ModuleAProp1;
    }

    public int getLevel2ModuleAProp2()
    {
      return level2ModuleAProp2;
    }

    public void setLevel2ModuleAProp2(int level2ModuleAProp2)
    {
      this.level2ModuleAProp2 = level2ModuleAProp2;
    }

    public int getLevel2ModuleAProp3()
    {
      return level2ModuleAProp3;
    }

    public void setLevel2ModuleAProp3(int level2ModuleAProp3)
    {
      this.level2ModuleAProp3 = level2ModuleAProp3;
    }
  }

  public static class Level2ModuleB implements Module
  {
    private int level2ModuleBProp1 = 0;
    private int level2ModuleBProp2 = 0;
    private int level2ModuleBProp3 = 0;

    @InputPortFieldAnnotation(optional = true)
    public transient final ProxyInputPort<Integer> mIn = new ProxyInputPort<>();

    @OutputPortFieldAnnotation(optional = true)
    public transient final ProxyOutputPort<Integer> mOut1 = new ProxyOutputPort<>();

    @OutputPortFieldAnnotation(optional = true)
    public transient final ProxyOutputPort<Integer> mOut2 = new ProxyOutputPort<>();

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      DummyOperator o1 = dag.addOperator("O1", new DummyOperator());
      o1.setOperatorProp(level2ModuleBProp1);

      Level1Module m1 = dag.addModule("M1", new Level1Module());
      m1.setLevel1ModuleProp(level2ModuleBProp2);

      DummyOperator o2 = dag.addOperator("O2", new DummyOperator());
      o2.setOperatorProp(level2ModuleBProp3);

      dag.addStream("O1_M1", o1.out1, m1.mIn);
      dag.addStream("O1_O2", o1.out2, o2.in);

      mIn.set(o1.in);
      mOut1.set(m1.mOut);
      mOut2.set(o2.out1);
    }

    public int getLevel2ModuleBProp1()
    {
      return level2ModuleBProp1;
    }

    public void setLevel2ModuleBProp1(int level2ModuleBProp1)
    {
      this.level2ModuleBProp1 = level2ModuleBProp1;
    }

    public int getLevel2ModuleBProp2()
    {
      return level2ModuleBProp2;
    }

    public void setLevel2ModuleBProp2(int level2ModuleBProp2)
    {
      this.level2ModuleBProp2 = level2ModuleBProp2;
    }

    public int getLevel2ModuleBProp3()
    {
      return level2ModuleBProp3;
    }

    public void setLevel2ModuleBProp3(int level2ModuleBProp3)
    {
      this.level2ModuleBProp3 = level2ModuleBProp3;
    }
  }

  public static class ModuleAppExtreme implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      DummyInputOperator o1 = dag.addOperator("O1", new DummyInputOperator());
      o1.setInputOperatorProp(1);

      DummyOperator o2 = dag.addOperator("O2", new DummyOperator());
      o2.setOperatorProp(2);

      Level2ModuleA ma = dag.addModule("Ma", new Level2ModuleA());
      ma.setLevel2ModuleAProp1(11);
      ma.setLevel2ModuleAProp2(12);
      ma.setLevel2ModuleAProp3(13);

      Level2ModuleB mb = dag.addModule("Mb", new Level2ModuleB());
      mb.setLevel2ModuleBProp1(21);
      mb.setLevel2ModuleBProp2(22);
      mb.setLevel2ModuleBProp3(23);

      Level2ModuleA mc = dag.addModule("Mc", new Level2ModuleA());
      mc.setLevel2ModuleAProp1(31);
      mc.setLevel2ModuleAProp2(32);
      mc.setLevel2ModuleAProp3(33);

      Level2ModuleB md = dag.addModule("Md", new Level2ModuleB());
      md.setLevel2ModuleBProp1(41);
      md.setLevel2ModuleBProp2(42);
      md.setLevel2ModuleBProp3(43);

      dag.addStream("O1_O2", o1.out, o2.in);
      dag.addStream("O2_Ma", o2.out1, ma.mIn);
      dag.addStream("Ma_Mb", ma.mOut1, mb.mIn);
      dag.addStream("Ma_Md", ma.mOut2, md.mIn);
      dag.addStream("Mb_Mc", mb.mOut2, mc.mIn);
    }
  }

  @Test
  public void testModuleExtreme()
  {
    StreamingApplication app = new ModuleAppExtreme();
    Configuration conf = new Configuration(false);
    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(conf);
    LogicalPlan dag = new LogicalPlan();
    lpc.prepareDAG(dag, app, "ModuleApp");

    dag.validate();
    validateTopLevelStreams(dag);
    validateTopLevelOperators(dag);
  }

  private void validateTopLevelStreams(LogicalPlan dag)
  {
    List<String> streamNames = new ArrayList<>();
    for (LogicalPlan.StreamMeta streamMeta : dag.getAllStreams()) {
      streamNames.add(streamMeta.getName());
    }

    Assert.assertTrue(streamNames.contains("Mb_O1_M1"));
    Assert.assertTrue(streamNames.contains("O2_Ma"));
    Assert.assertTrue(streamNames.contains("Mb_Mc"));
    Assert.assertTrue(streamNames.contains("Mb_O1_O2"));
    Assert.assertTrue(streamNames.contains("Ma_M1_M2&O1"));
    Assert.assertTrue(streamNames.contains("Md_O1_M1"));
    Assert.assertTrue(streamNames.contains("Ma_Md"));
    Assert.assertTrue(streamNames.contains("Mc_M1_M2&O1"));
    Assert.assertTrue(streamNames.contains("Md_O1_O2"));
    Assert.assertTrue(streamNames.contains("Ma_Mb"));
    Assert.assertTrue(streamNames.contains("O1_O2"));

    validateSeperateStream(dag, "Mb_O1_M1", "Mb_O1", "Mb_M1_O1");
    validateSeperateStream(dag, "O2_Ma", "O2", "Ma_M1_O1");
    validateSeperateStream(dag, "Mb_Mc", "Mb_O2", "Mc_M1_O1");
    validateSeperateStream(dag, "Mb_O1_O2", "Mb_O1", "Mb_O2");
    validateSeperateStream(dag, "Ma_M1_M2&O1", "Ma_M1_O1", "Ma_O1", "Ma_M2_O1");
    validateSeperateStream(dag, "Md_O1_M1", "Md_O1", "Md_M1_O1");
    validateSeperateStream(dag, "Ma_Md", "Ma_O1", "Md_O1");
    validateSeperateStream(dag, "Mc_M1_M2&O1", "Mc_M1_O1", "Mc_O1", "Mc_M2_O1");
    validateSeperateStream(dag, "Md_O1_O2", "Md_O1", "Md_O2");
    validateSeperateStream(dag, "Ma_Mb", "Ma_M2_O1", "Mb_O1");
    validateSeperateStream(dag, "O1_O2", "O1", "O2");
    validateSeperateStream(dag, "O1_O2", "O1", "O2");
  }

  private void validateSeperateStream(LogicalPlan dag, String streamName, String inputOperatorName, String... outputOperatorNames)
  {
    LogicalPlan.StreamMeta streamMeta = dag.getStream(streamName);
    String sourceName = streamMeta.getSource().getOperatorMeta().getName();

    List<String> sinksName = new ArrayList<>();
    for (LogicalPlan.InputPortMeta inputPortMeta : streamMeta.getSinks()) {
      sinksName.add(inputPortMeta.getOperatorWrapper().getName());
    }

    Assert.assertTrue(inputOperatorName.equals(sourceName));
    Assert.assertEquals(outputOperatorNames.length, sinksName.size());

    for (String outputOperatorName : outputOperatorNames) {
      Assert.assertTrue(sinksName.contains(outputOperatorName));
    }
  }

  private void validateTopLevelOperators(LogicalPlan dag)
  {
    List<String> operatorNames = new ArrayList<>();
    for (LogicalPlan.OperatorMeta operatorMeta : dag.getAllOperators()) {
      operatorNames.add(operatorMeta.getName());
    }
    Assert.assertTrue(operatorNames.contains("O1"));
    Assert.assertTrue(operatorNames.contains("O2"));
    Assert.assertTrue(operatorNames.contains("Ma_M1_O1"));
    Assert.assertTrue(operatorNames.contains("Ma_M2_O1"));
    Assert.assertTrue(operatorNames.contains("Ma_O1"));
    Assert.assertTrue(operatorNames.contains("Mb_O1"));
    Assert.assertTrue(operatorNames.contains("Mb_M1_O1"));
    Assert.assertTrue(operatorNames.contains("Mb_O2"));
    Assert.assertTrue(operatorNames.contains("Mc_M1_O1"));
    Assert.assertTrue(operatorNames.contains("Mc_M2_O1"));
    Assert.assertTrue(operatorNames.contains("Mc_O1"));
    Assert.assertTrue(operatorNames.contains("Md_O1"));
    Assert.assertTrue(operatorNames.contains("Md_M1_O1"));
    Assert.assertTrue(operatorNames.contains("Md_O2"));

    validateOperatorPropertyValue(dag, "O1", 1);
    validateOperatorPropertyValue(dag, "O2", 2);
    validateOperatorPropertyValue(dag, "Ma_M1_O1", 11);
    validateOperatorPropertyValue(dag, "Ma_M2_O1", 12);
    validateOperatorPropertyValue(dag, "Ma_O1", 13);
    validateOperatorPropertyValue(dag, "Mb_O1", 21);
    validateOperatorPropertyValue(dag, "Mb_M1_O1", 22);
    validateOperatorPropertyValue(dag, "Mb_O2", 23);
    validateOperatorPropertyValue(dag, "Mc_M1_O1", 31);
    validateOperatorPropertyValue(dag, "Mc_M2_O1", 32);
    validateOperatorPropertyValue(dag, "Mc_O1", 33);
    validateOperatorPropertyValue(dag, "Md_O1", 41);
    validateOperatorPropertyValue(dag, "Md_M1_O1", 42);
    validateOperatorPropertyValue(dag, "Md_O2", 43);

    validateOperatorParent(dag, "O1", null);
    validateOperatorParent(dag, "O2", null);
    validateOperatorParent(dag, "Ma_M1_O1", "Ma_M1");
    validateOperatorParent(dag, "Ma_M2_O1", "Ma_M2");
    validateOperatorParent(dag, "Ma_O1", "Ma");
    validateOperatorParent(dag, "Mb_O1", "Mb");
    validateOperatorParent(dag, "Mb_M1_O1", "Mb_M1");
    validateOperatorParent(dag, "Mb_O2", "Mb");
    validateOperatorParent(dag, "Mc_M1_O1", "Mc_M1");
    validateOperatorParent(dag, "Mc_M2_O1", "Mc_M2");
    validateOperatorParent(dag, "Mc_O1", "Mc");
    validateOperatorParent(dag, "Md_O1", "Md");
    validateOperatorParent(dag, "Md_M1_O1", "Md_M1");
    validateOperatorParent(dag, "Md_O2", "Md");
  }

  private void validateOperatorParent(LogicalPlan dag, String operatorName, String parentModuleName)
  {
    LogicalPlan.OperatorMeta operatorMeta = dag.getOperatorMeta(operatorName);
    if (parentModuleName == null) {
      Assert.assertNull(operatorMeta.getParentModuleName());
    } else {
      Assert.assertTrue(parentModuleName.equals(operatorMeta.getParentModuleName()));
    }
  }

  void validateOperatorPropertyValue(LogicalPlan dag, String operatorName, int expectedValue)
  {
    LogicalPlan.OperatorMeta oMeta = dag.getOperatorMeta(operatorName);
    if (operatorName.equals("O1")) {
      DummyInputOperator operator = (DummyInputOperator)oMeta.getOperator();
      Assert.assertEquals(expectedValue, operator.getInputOperatorProp());
    } else {
      DummyOperator operator = (DummyOperator)oMeta.getOperator();
      Assert.assertEquals(expectedValue, operator.getOperatorProp());
    }
  }

}
