package com.datatorrent.api.plan;

import com.datatorrent.api.DAG.StreamMeta;
import com.datatorrent.api.Operator;

public interface PlanModifier
{
  public void addOperator(String name, Operator operator);

  public void removeOperator(String name);

  public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<?>... sinks);

  public void addStream(String streamName, String sourceOperName, String sourcePortName, String targetOperName, String targetPortName);

  public void removeStream(String streamName);

  public void addSink(String streamName, String targetOperName, String targetPortName);

  public StreamMeta addSinks(String id, Operator.InputPort<?>... sinks);

  public void setOperatorProperty(String operatorName, String propertyName, String propertyValue);
}
