package com.datatorrent.stram.moduleexperiment;

import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Module;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.DefaultInputProxyPort;
import com.datatorrent.api.Operator.DefaultOutputProxyPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.ModuleMeta;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

public class ModuleTest
{

  /*
   * Input Operator - 1
   */
  public static class DummyInputOperator extends BaseOperator implements InputOperator {

    Random r = new Random();
    public transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();

    @Override
    public void emitTuples()
    {
      output.emit(r.nextInt());
    }
  }

  /*
   * Operator - 2
   */
  public static class DummyOperator extends BaseOperator {
    int prop1;

    public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>() {
      @Override
      public void process(Integer tuple)
      {
        LOG.info(tuple.intValue()+" processed");
      }
    };
    public transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();
  }

  /*
   * Module Definition
   */
  public static class TestModule implements Module {

    public transient DefaultInputProxyPort<Integer> moduleInput = new Operator.DefaultInputProxyPort<Integer>();
    public transient DefaultOutputProxyPort<Integer> moduleOutput = new Operator.DefaultOutputProxyPort<Integer>();

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      LOG.info("Module - PopulateDAG");
      DummyOperator dummyOperator = dag.addOperator("DummyOperator", new DummyOperator());
      moduleInput.setInputPort(dummyOperator.input);
      moduleOutput.setOutputPort(dummyOperator.output);
    }
  }

  @Test
  public void moduleTest(){

    /*
     * Streaming App
     */
    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        LOG.info("Application - PopulateDAG");
        DummyInputOperator dummyInputOperator = dag.addOperator("DummyInputOperator", new DummyInputOperator());
        Module m1 = dag.addModule("TestModule1", new TestModule());
        Module m2 = dag.addModule("TestModule2", new TestModule());
        DummyOperator dummyOutputOperator = dag.addOperator("DummyOutputOperator", new DummyOperator());
        dag.addStream("Operator To Module", dummyInputOperator.output, ((TestModule)m1).moduleInput);
        dag.addStream("Module To Module", ((TestModule)m1).moduleOutput, ((TestModule)m2).moduleInput);
        dag.addStream("Module To Operator", ((TestModule)m2).moduleOutput, dummyOutputOperator.input);
      }
    };

    Configuration conf = new Configuration(false);
//    conf.addResource(StramClientUtils.DT_SITE_XML_FILE);
    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(conf);
    LogicalPlan dag = new LogicalPlan();
    lpc.prepareDAG(dag, app, "TestApp");

    Assert.assertEquals(dag.getAllModules().size(), 2);
    Assert.assertEquals(dag.getAllOperators().size(), 4);
    Assert.assertEquals(dag.getAllStreams().size(), 3);
  }

  private static Logger LOG = LoggerFactory.getLogger(ModuleTest.class);
}
