package com.datatorrent.stram.moduleexperiment;

import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
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

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      LOG.info("Module - PopulateDAG");
      DummyOperator dummyOperator = dag.addOperator("DummyOperator", new DummyOperator());
      moduleInput.setInputPort(dummyOperator.input);
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
        Module m = dag.addModule("TestModule", new TestModule());
        dag.addStream("Operator To Module", dummyInputOperator.output, ((TestModule)m).moduleInput);
      }
    };

    Configuration conf = new Configuration(false);
//    conf.addResource(StramClientUtils.DT_SITE_XML_FILE);
    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(conf);
    LogicalPlan dag = new LogicalPlan();
    lpc.prepareDAG(dag, app, "TestApp");
    System.out.println(dag);
    System.out.println(dag.getAllOperators());
    System.out.println(dag.getAllStreams());
    LOG.info("Populating {} modules", dag.getAllModules().size());
    Iterator<ModuleMeta> i = dag.getAllModules().iterator();
    while(i.hasNext()){
      Module m = i.next().getModule();
      m.populateDAG(dag, conf);
      System.out.println(dag);
      System.out.println(dag.getAllOperators());
      System.out.println(dag.getAllStreams().iterator().next());
    }
    dag.applyStreamLinks();
  }

  private static Logger LOG = LoggerFactory.getLogger(ModuleTest.class);
}
