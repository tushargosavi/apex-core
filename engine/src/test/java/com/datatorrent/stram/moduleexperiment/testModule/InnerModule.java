package com.datatorrent.stram.moduleexperiment.testModule;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.api.Operator.ProxyInputPort;
import com.datatorrent.api.Operator.ProxyOutputPort;

public class InnerModule implements Module
{

  public transient ProxyInputPort<Integer> mInput = new ProxyInputPort<Integer>();
  public transient ProxyOutputPort<Integer> mOutputEven = new ProxyOutputPort<Integer>();
  public transient ProxyOutputPort<Integer> mOutputOdd = new ProxyOutputPort<Integer>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    OddEvenOperator moduleOperator = dag.addOperator("InnerModule", new OddEvenOperator());
    mInput.set(moduleOperator.input);
    mOutputEven.set(moduleOperator.even);
    mOutputOdd.set(moduleOperator.odd);
  }
}
