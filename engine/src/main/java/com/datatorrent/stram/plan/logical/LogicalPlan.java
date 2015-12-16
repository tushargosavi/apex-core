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

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.ValidationException;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Sets;
import com.sun.org.apache.xpath.internal.operations.Mod;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.DAG;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Module;
import com.datatorrent.api.Module.ProxyInputPort;
import com.datatorrent.api.Module.ProxyOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.StringCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.experimental.AppData;
import com.datatorrent.common.metric.MetricsAggregator;
import com.datatorrent.common.metric.SingleMetricAggregator;
import com.datatorrent.common.metric.sum.DoubleSumAggregator;
import com.datatorrent.common.metric.sum.LongSumAggregator;
import com.datatorrent.common.util.FSStorageAgent;
import com.datatorrent.stram.engine.DefaultUnifier;
import com.datatorrent.stram.engine.Slider;

/**
 * DAG contains the logical declarations of operators and streams.
 * <p>
 * Operators have ports that are connected through streams. Ports can be
 * mandatory or optional with respect to their need to connect a stream to it.
 * Each port can be connected to a single stream only. A stream has to be
 * connected to one output port and can go to multiple input ports.
 * <p>
 * The DAG will be serialized and deployed to the cluster, where it is translated
 * into the physical plan.
 *
 * @since 0.3.2
 */
public class LogicalPlan implements Serializable, DAG
{
  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  private static final long serialVersionUID = -2099729915606048704L;
  private static final Logger LOG = LoggerFactory.getLogger(LogicalPlan.class);
  // The name under which the application master expects its configuration.
  public static final String SER_FILE_NAME = "dt-conf.ser";
  public static final String LAUNCH_CONFIG_FILE_NAME = "dt-launch-config.xml";
  private static final transient AtomicInteger logicalOperatorSequencer = new AtomicInteger();
  public static final String MODULE_NAMESPACE_SEPARATOR = "$";
  private boolean expanded = false;

  /**
   * Constant
   * <code>SUBDIR_CHECKPOINTS="checkpoints"</code>
   */
  public static String SUBDIR_CHECKPOINTS = "checkpoints";
  /**
   * Constant
   * <code>SUBDIR_STATS="stats"</code>
   */
  public static String SUBDIR_STATS = "stats";
  /**
   * Constant
   * <code>SUBDIR_EVENTS="events"</code>
   */
  public static String SUBDIR_EVENTS = "events";

  /**
   * A flag to specify whether to use the fast publisher or not. This attribute was moved
   * from DAGContext. This can be here till the fast publisher is fully tested and working as desired.
   * Then it can be moved back to DAGContext.
   */
  public static Attribute<Boolean> FAST_PUBLISHER_SUBSCRIBER = new Attribute<Boolean>(false);
  public static Attribute<Long> HDFS_TOKEN_LIFE_TIME = new Attribute<Long>(604800000l);
  public static Attribute<Long> RM_TOKEN_LIFE_TIME = new Attribute<Long>(YarnConfiguration.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
  public static Attribute<String> KEY_TAB_FILE = new Attribute<String>((String) null, new StringCodec.String2String());
  public static Attribute<Double> TOKEN_REFRESH_ANTICIPATORY_FACTOR = new Attribute<Double>(0.7);
  /**
   * Comma separated list of jar file dependencies to be deployed with the application.
   * The launcher will combine the list with built-in dependencies and those specified
   * that are made available through the distributed file system to application master
   * and child containers.
   */
  public static Attribute<String> LIBRARY_JARS = new Attribute<String>(new StringCodec.String2String());
  /**
   * Comma separated list of archives to be deployed with the application.
   * The launcher will include the archives into the final set of resources
   * that are made available through the distributed file system to application master
   * and child containers.
   */
  public static Attribute<String> ARCHIVES = new Attribute<String>(new StringCodec.String2String());
  /**
   * Comma separated list of files to be deployed with the application. The launcher will include the files into the
   * final set of resources that are made available through the distributed file system to application master and child
   * containers.
   */
  public static Attribute<String> FILES = new Attribute<String>(new StringCodec.String2String());
  /**
   * The maximum number of containers (excluding the application master) that the application is allowed to request.
   * If the DAG plan requires less containers, remaining count won't be allocated from the resource manager.
   * Example: DAG with several operators and all streams container local would require one container,
   * only one container will be requested from the resource manager.
   */
  public static Attribute<Integer> CONTAINERS_MAX_COUNT = new Attribute<Integer>(Integer.MAX_VALUE);

  /**
   * The application attempt ID from YARN
   */
  public static Attribute<Integer> APPLICATION_ATTEMPT_ID = new Attribute<>(1);

  static {
    Attribute.AttributeMap.AttributeInitializer.initialize(LogicalPlan.class);
  }

  private final Map<String, StreamMeta> streams = new HashMap<String, StreamMeta>();
  private final Map<String, OperatorMeta> operators = new HashMap<String, OperatorMeta>();
  public final Map<String, ModuleMeta> modules = new LinkedHashMap<>();
  private final List<OperatorMeta> rootOperators = new ArrayList<OperatorMeta>();
  private final Attribute.AttributeMap attributes = new DefaultAttributeMap();
  private transient int nodeIndex = 0; // used for cycle validation
  private transient Stack<OperatorMeta> stack = new Stack<OperatorMeta>(); // used for cycle validation
  private transient Map<String, ArrayListMultimap<OutputPort<?>, InputPort<?>>> streamLinks = new HashMap<String, ArrayListMultimap<Operator.OutputPort<?>, Operator.InputPort<?>>>();

  @Override
  public Attribute.AttributeMap getAttributes()
  {
    return attributes;
  }

  @Override
  public <T> T getValue(Attribute<T> key)
  {
    T val = attributes.get(key);
    if (val == null) {
      return key.defaultValue;
    }

    return val;
  }

  public LogicalPlan()
  {
  }

  @Override
  public void setCounters(Object counters)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void sendMetrics(Collection<String> metricNames)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public final class InputPortMeta implements DAG.InputPortMeta, Serializable
  {
    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 1L;
    private NodeMeta nodeMeta;
    private String fieldName;
    private InputPortFieldAnnotation portAnnotation;
    private AppData.QueryPort adqAnnotation;
    private final Attribute.AttributeMap attributes = new DefaultAttributeMap();
    //This is null when port is not hidden
    private Class<?> classDeclaringHiddenPort;

    public NodeMeta getNodeMeta() { return nodeMeta; }
    
    public String getPortName()
    {
      return fieldName;
    }

    public InputPort getPortObject()
    {
      for (Map.Entry<Operator.InputPort<?>, InputPortMeta> e : nodeMeta.getPortMapping().inPortMap.entrySet()) {
        if (e.getValue() == this) {
          return e.getKey();
        }
      }

      throw new AssertionError("Cannot find the port object for " + this);
    }

    public boolean isAppDataQueryPort()
    {
      return adqAnnotation != null;
    }

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
              append("operator", this.nodeMeta).
              append("portAnnotation", this.portAnnotation).
              append("adqAnnotation", this.adqAnnotation).
              append("field", this.fieldName).
              toString();
    }

    @Override
    public Attribute.AttributeMap getAttributes()
    {
      return attributes;
    }

    @Override
    public <T> T getValue(Attribute<T> key)
    {
      T attr = attributes.get(key);
      if (attr == null) {
        return key.defaultValue;
      }

      return attr;
    }

    @Override
    public void setCounters(Object counters)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void sendMetrics(Collection<String> metricNames)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public OperatorMeta getOperatorWrapper() {
      return (OperatorMeta)nodeMeta;
    }
  }

  public final class OutputPortMeta implements DAG.OutputPortMeta, Serializable
  {
    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201412091633L;
    NodeMeta nodeMeta;
    private OperatorMeta unifierMeta;
    private OperatorMeta sliderMeta;
    private String fieldName;
    private OutputPortFieldAnnotation portAnnotation;
    private AppData.ResultPort adrAnnotation;
    private final DefaultAttributeMap attributes;
    //This is null when port is not hidden
    private Class<?> classDeclaringHiddenPort;

    public OutputPortMeta()
    {
      this.attributes = new DefaultAttributeMap();
    }

    public NodeMeta getNodeMeta() { return nodeMeta; }

    public OperatorMeta getOperatorMeta() {
      if (nodeMeta instanceof OperatorMeta) {
        return (OperatorMeta)nodeMeta;
      }
      return null;
    }

    @Override
    public OperatorMeta getUnifierMeta()
    {
      if (unifierMeta == null) {
        unifierMeta = new OperatorMeta(nodeMeta.getName() + '.' + fieldName + "#unifier",  getUnifier());
      }

      return unifierMeta;
    }

    public OperatorMeta getSlidingUnifier(int numberOfBuckets, int slidingApplicationWindowCount, int numberOfSlidingWindows)
    {
      if (sliderMeta == null) {
        @SuppressWarnings("unchecked")
        Slider slider = new Slider((Unifier<Object>) getUnifier(), numberOfBuckets, numberOfSlidingWindows);
        try {
          sliderMeta = new OperatorMeta(nodeMeta.getName() + '.' + fieldName + "#slider", slider, getUnifierMeta().attributes.clone());
        }
        catch (CloneNotSupportedException ex) {
          throw new RuntimeException(ex);
        }
        sliderMeta.getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, slidingApplicationWindowCount);
      }
      return sliderMeta;
    }

    public String getPortName()
    {
      return fieldName;
    }

    public OutputPort<?> getPortObject() {
      if (nodeMeta != null) {
        for (Map.Entry<OutputPort<?>, OutputPortMeta> e : nodeMeta.getPortMapping().outPortMap.entrySet()) {
          if (e.getValue() == this) {
            return e.getKey();
          }
        }
      }

      throw new AssertionError("Cannot find the port object for " + this);
    }

    public Operator.Unifier<?> getUnifier() {
      for (Map.Entry<OutputPort<?>, OutputPortMeta> e : nodeMeta.getPortMapping().outPortMap.entrySet()) {
        if (e.getValue() == this) {
          Unifier<?> unifier = e.getKey().getUnifier();
          if (unifier == null) {
            break;
          }
          LOG.debug("User supplied unifier is {}", unifier);
          return unifier;
        }
      }

      LOG.debug("Using default unifier for {}", this);
      return new DefaultUnifier();
    }

    @Override
    public Attribute.AttributeMap getAttributes()
    {
      return attributes;
    }

    @Override
    public <T> T getValue(Attribute<T> key)
    {
      T attr = attributes.get(key);
      if (attr == null) {
        return key.defaultValue;
      }

      return attr;
    }

    public boolean isAppDataResultPort()
    {
      return adrAnnotation != null;
    }

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
              append("operator", this.nodeMeta).
              append("portAnnotation", this.portAnnotation).
              append("adrAnnotation", this.adrAnnotation).
              append("field", this.fieldName).
              toString();
    }

    @Override
    public void setCounters(Object counters)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void sendMetrics(Collection<String> metricNames)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

  }

  /**
   * Representation of streams in the logical layer. Instances are created through {@link LogicalPlan#addStream}.
   */
  public final class StreamMeta implements DAG.StreamMeta, Serializable
  {
    private static final long serialVersionUID = 1L;
    private Locality locality;
    private final List<InputPortMeta> sinks = new ArrayList<InputPortMeta>();
    private OutputPortMeta source;
    private final String id;
    private OperatorMeta persistOperatorForStream;
    private InputPortMeta persistOperatorInputPort;
    private Set<InputPortMeta> enableSinksForPersisting;
    private String persistOperatorName;
    public Map<InputPortMeta, OperatorMeta> sinkSpecificPersistOperatorMap;
    public Map<InputPortMeta, InputPortMeta> sinkSpecificPersistInputPortMap;
    private String moduleName;  // Name of the module which has this stream. null if top level stream.

    private StreamMeta(String id)
    {
      this.id = id;
      enableSinksForPersisting = new HashSet<InputPortMeta>();
      sinkSpecificPersistOperatorMap = new HashMap<LogicalPlan.InputPortMeta, OperatorMeta>();
      sinkSpecificPersistInputPortMap = new HashMap<LogicalPlan.InputPortMeta, InputPortMeta>();
    }

    @Override
    public String getName()
    {
      return id;
    }

    @Override
    public Locality getLocality()
    {
      return this.locality;
    }

    @Override
    public StreamMeta setLocality(Locality locality)
    {
      this.locality = locality;
      return this;
    }

    public String getModuleName()
    {
      return moduleName;
    }

    public void setModuleName(String moduleName)
    {
      this.moduleName = moduleName;
    }

    public <T> OutputPortMeta getSource()
    {
      return source;
    }

    @Override
    public StreamMeta setSource(Operator.OutputPort<?> port)
    {
      OutputPortMeta portMeta = assertGetPortMeta(port);
      NodeMeta om = portMeta.getNodeMeta();
      if (om != null) {
        if (om.outputStreams.containsKey(portMeta)) {
          String msg = String.format("Operator %s already connected to %s", om.name, om.outputStreams.get(portMeta));
          throw new IllegalArgumentException(msg);
        }
        this.source = portMeta;
        om.outputStreams.put(portMeta, this);
        return this;
      }

      return null;
    }

    public List<InputPortMeta> getSinks()
    {
      return sinks;
    }

    @Override
    public StreamMeta addSink(Operator.InputPort<?> port)
    {
      InputPortMeta portMeta = assertGetPortMeta(port);
      NodeMeta om = portMeta.getNodeMeta();
      String portName = portMeta.getPortName();
      if (om != null) {
        if (om.inputStreams.containsKey(portMeta)) {
          throw new IllegalArgumentException(String.format("Port %s already connected to stream %s", portName, om.inputStreams.get(portMeta)));
        }
        om.inputStreams.put(portMeta, this);
      }

      /*
      finalizeValidate(portMeta);
      */
      if (source.getPortObject() instanceof ProxyOutputPort || port instanceof ProxyInputPort) {
        boolean s1 = source.getPortObject() instanceof ProxyOutputPort;
        boolean s2 = port instanceof ProxyInputPort;
        System.out.println("id " + id + " source " + s1 + " sink " + s2);
        ArrayListMultimap<Operator.OutputPort<?>, Operator.InputPort<?>> streamMap = streamLinks.get(id);
        if (streamMap == null) {
          System.out.println("Adding element in streamMap " + id);
          streamMap = ArrayListMultimap.create();
          streamLinks.put(id, streamMap);
        }
        System.out.println("Adding element in arrylist " + id);
        streamMap.put(source.getPortObject(), portMeta.getPortObject());
      } else {
        sinks.add(portMeta);
        rootOperators.remove(portMeta.nodeMeta);
      }
      return this;
    }

    public void remove()
    {
      for (InputPortMeta ipm : this.sinks) {
        ipm.getNodeMeta().inputStreams.remove(ipm);
        if (ipm.getNodeMeta().inputStreams.isEmpty()) {
          rootOperators.add((OperatorMeta)ipm.getNodeMeta());
        }
      }
      // Remove persist operator for at stream level if present:
      if (getPersistOperator() != null) {
        removeOperator(getPersistOperator().getOperator());
      }

      // Remove persist operators added for specific sinks :
      if (!sinkSpecificPersistOperatorMap.isEmpty()) {
        for (Entry<InputPortMeta, OperatorMeta> entry : sinkSpecificPersistOperatorMap.entrySet()) {
          removeOperator(entry.getValue().getOperator());
        }
        sinkSpecificPersistOperatorMap.clear();
        sinkSpecificPersistInputPortMap.clear();
      }
      this.sinks.clear();
      if (this.source != null) {
        this.source.getNodeMeta().outputStreams.remove(this.source);
      }
      this.source = null;
      streams.remove(this.id);
    }

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
              append("id", this.id).
              toString();
    }

    @Override
    public int hashCode()
    {
      int hash = 7;
      hash = 31 * hash + (this.locality != null ? this.locality.hashCode() : 0);
      hash = 31 * hash + (this.source != null ? this.source.hashCode() : 0);
      hash = 31 * hash + (this.id != null ? this.id.hashCode() : 0);
      return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final StreamMeta other = (StreamMeta)obj;
      if (this.locality != other.locality) {
        return false;
      }
      if (this.sinks != other.sinks && (this.sinks == null || !this.sinks.equals(other.sinks))) {
        return false;
      }
      if (this.source != other.source && (this.source == null || !this.source.equals(other.source))) {
        return false;
      }
      return !((this.id == null) ? (other.id != null) : !this.id.equals(other.id));
    }

    @Override
    public StreamMeta persistUsing(String name, Operator persistOperator, InputPort<?> port)
    {
      persistOperatorName = name;
      enablePersistingForSinksAddedSoFar(persistOperator);
      OperatorMeta persistOpMeta = createPersistOperatorMeta(persistOperator);
      if (!persistOpMeta.getPortMapping().inPortMap.containsKey(port)) {
        String msg = String.format("Port argument %s does not belong to persist operator passed %s", port, persistOperator);
        throw new IllegalArgumentException(msg);
      }

      setPersistOperatorInputPort(persistOpMeta.getPortMapping().inPortMap.get(port));

      return this;
    }

    @Override
    public StreamMeta persistUsing(String name, Operator persistOperator)
    {
      persistOperatorName = name;
      enablePersistingForSinksAddedSoFar(persistOperator);
      OperatorMeta persistOpMeta = createPersistOperatorMeta(persistOperator);
      InputPortMeta port = persistOpMeta.getPortMapping().inPortMap.values().iterator().next();
      setPersistOperatorInputPort(port);
      return this;
    }

    private void enablePersistingForSinksAddedSoFar(Operator persistOperator)
    {
      for (InputPortMeta portMeta : getSinks()) {
        enableSinksForPersisting.add(portMeta);
      }
    }

    private OperatorMeta createPersistOperatorMeta(Operator persistOperator)
    {
      addOperator(persistOperatorName, persistOperator);
      OperatorMeta persistOpMeta = getOperatorMeta(persistOperatorName);
      setPersistOperator(persistOpMeta);
      if (persistOpMeta.getPortMapping().inPortMap.isEmpty()) {
        String msg = String.format("Persist operator passed %s has no input ports to connect", persistOperator);
        throw new IllegalArgumentException(msg);
      }
      Map<InputPort<?>, InputPortMeta> inputPortMap = persistOpMeta.getPortMapping().inPortMap;
      int nonOptionalInputPortCount = 0;
      for (InputPortMeta inputPort : inputPortMap.values()) {
        if (inputPort.portAnnotation == null || !inputPort.portAnnotation.optional()) {
          // By default input port is non-optional unless specified
          nonOptionalInputPortCount++;
        }
      }

      if (nonOptionalInputPortCount > 1) {
        String msg = String.format("Persist operator %s has more than 1 non optional input port", persistOperator);
        throw new IllegalArgumentException(msg);
      }

      Map<OutputPort<?>, OutputPortMeta> outputPortMap = persistOpMeta.getPortMapping().outPortMap;
      for (OutputPortMeta outPort : outputPortMap.values()) {
        if (outPort.portAnnotation != null && !outPort.portAnnotation.optional()) {
          // By default output port is optional unless specified
          String msg = String.format("Persist operator %s has non optional output port %s", persistOperator, outPort.fieldName);
          throw new IllegalArgumentException(msg);
        }
      }
      return persistOpMeta;
    }

    public OperatorMeta getPersistOperator()
    {
      return persistOperatorForStream;
    }

    private void setPersistOperator(OperatorMeta persistOperator)
    {
      this.persistOperatorForStream = persistOperator;
    }

    public InputPortMeta getPersistOperatorInputPort()
    {
      return persistOperatorInputPort;
    }

    private void setPersistOperatorInputPort(InputPortMeta inport)
    {
      this.addSink(inport.getPortObject());
      this.persistOperatorInputPort = inport;
    }

    public Set<InputPortMeta> getSinksToPersist()
    {
      return enableSinksForPersisting;
    }

    private String getPersistOperatorName(Operator operator)
    {
      return id + "_persister";
    }

    private String getPersistOperatorName(InputPort<?> sinkToPersist)
    {
      InputPortMeta portMeta = assertGetPortMeta(sinkToPersist);
      NodeMeta operatorMeta = portMeta.getNodeMeta();
      return id + "_" + operatorMeta.getName() + "_persister";
    }

    @Override
    public StreamMeta persistUsing(String name, Operator persistOperator, InputPort<?> port, InputPort<?> sinkToPersist)
    {
      // When persist Stream is invoked for a specific sink, persist operator can directly be added
      String persistOperatorName = name;
      addOperator(persistOperatorName, persistOperator);
      addSink(port);
      InputPortMeta sinkPortMeta = assertGetPortMeta(sinkToPersist);
      addStreamCodec(sinkPortMeta, port);
      updateSinkSpecificPersistOperatorMap(sinkPortMeta, persistOperatorName, port);
      return this;
    }

    private void addStreamCodec(InputPortMeta sinkToPersistPortMeta, InputPort<?> port)
    {
      StreamCodec<Object> inputStreamCodec = sinkToPersistPortMeta.getValue(PortContext.STREAM_CODEC) != null ? (StreamCodec<Object>) sinkToPersistPortMeta.getValue(PortContext.STREAM_CODEC) : (StreamCodec<Object>) sinkToPersistPortMeta.getPortObject().getStreamCodec();
      if (inputStreamCodec != null) {
        Map<InputPortMeta, StreamCodec<Object>> codecs = new HashMap<InputPortMeta, StreamCodec<Object>>();
        codecs.put(sinkToPersistPortMeta, inputStreamCodec);
        InputPortMeta persistOperatorPortMeta = assertGetPortMeta(port);
        StreamCodec<Object> specifiedCodecForPersistOperator = (persistOperatorPortMeta.getValue(PortContext.STREAM_CODEC) != null) ? (StreamCodec<Object>) persistOperatorPortMeta.getValue(PortContext.STREAM_CODEC) : (StreamCodec<Object>) port.getStreamCodec();
        StreamCodecWrapperForPersistance<Object> codec = new StreamCodecWrapperForPersistance<Object>(codecs, specifiedCodecForPersistOperator);
        setInputPortAttribute(port, PortContext.STREAM_CODEC, codec);
      }
    }

    private void updateSinkSpecificPersistOperatorMap(InputPortMeta sinkToPersistPortMeta, String persistOperatorName, InputPort<?> persistOperatorInPort)
    {
      OperatorMeta persistOpMeta = operators.get(persistOperatorName);
      this.sinkSpecificPersistOperatorMap.put(sinkToPersistPortMeta, persistOpMeta);
      this.sinkSpecificPersistInputPortMap.put(sinkToPersistPortMeta, persistOpMeta.getMeta(persistOperatorInPort));
    }

    public void resetStreamPersistanceOnSinkRemoval(InputPortMeta sinkBeingRemoved)
    {
     /*
      * If persistStream was enabled for the entire stream and the operator
      * to be removed was the only one enabled for persisting, Remove the persist operator
      */
      if (enableSinksForPersisting.contains(sinkBeingRemoved)) {
        enableSinksForPersisting.remove(sinkBeingRemoved);
        if (enableSinksForPersisting.isEmpty()) {
          removeOperator(getPersistOperator().getOperator());
          setPersistOperator(null);
        }
      }

      // If persisting was added specific to this sink, remove the persist operator
      if (sinkSpecificPersistInputPortMap.containsKey(sinkBeingRemoved)) {
        sinkSpecificPersistInputPortMap.remove(sinkBeingRemoved);
      }
      if (sinkSpecificPersistOperatorMap.containsKey(sinkBeingRemoved)) {
        OperatorMeta persistOpMeta = sinkSpecificPersistOperatorMap.get(sinkBeingRemoved);
        sinkSpecificPersistOperatorMap.remove(sinkBeingRemoved);
        removeOperator(persistOpMeta.getOperator());
      }
    }

    private void copyFrom(StreamMeta meta) {
      this.locality = meta.getLocality();
    }
  }

  public class NodeMeta implements Serializable
  {
    protected final LinkedHashMap<InputPortMeta, StreamMeta> inputStreams = new LinkedHashMap<>();
    protected final LinkedHashMap<OutputPortMeta, StreamMeta> outputStreams = new LinkedHashMap<>();
    private final String name;
    private transient Integer nindex; // for cycle detection
    private transient Integer lowlink; // for cycle detection
    protected transient Object component;

    public NodeMeta(String name) {
      this.name = name;
    }

    public String getName()
    {
      return name;
    }

    public Object getComponent() {
      return component;
    }

    public void setComponent(Object component)
    {
      this.component = component;
    }

    private class PortMapping implements Operators.OperatorDescriptor
    {
      private final Map<Operator.InputPort<?>, InputPortMeta> inPortMap = new HashMap<>();
      private final Map<Operator.OutputPort<?>, OutputPortMeta> outPortMap = new HashMap<>();
      private final Map<String, Object> portNameMap = new HashMap<String, Object>();

      @Override
      public void addInputPort(InputPort<?> portObject, Field field, InputPortFieldAnnotation portAnnotation, AppData.QueryPort adqAnnotation)
      {
        if (!NodeMeta.this.inputStreams.isEmpty()) {
          for (Map.Entry<LogicalPlan.InputPortMeta, LogicalPlan.StreamMeta> e : NodeMeta.this.inputStreams.entrySet()) {
            LogicalPlan.InputPortMeta pm = e.getKey();
            if (pm.nodeMeta == NodeMeta.this && pm.fieldName.equals(field.getName())) {
              //LOG.debug("Found existing port meta for: " + field);
              inPortMap.put(portObject, pm);
              markInputPortIfHidden(pm.getPortName(), pm, field.getDeclaringClass());
              return;
            }
          }
        }
        InputPortMeta metaPort = new InputPortMeta();
        metaPort.nodeMeta = NodeMeta.this;
        metaPort.fieldName = field.getName();
        metaPort.portAnnotation = portAnnotation;
        metaPort.adqAnnotation = adqAnnotation;
        inPortMap.put(portObject, metaPort);
        markInputPortIfHidden(metaPort.getPortName(), metaPort, field.getDeclaringClass());
      }

      @Override
      public void addOutputPort(OutputPort<?> portObject, Field field, OutputPortFieldAnnotation portAnnotation, AppData.ResultPort adrAnnotation)
      {
        if (!NodeMeta.this.outputStreams.isEmpty()) {
          for (Map.Entry<LogicalPlan.OutputPortMeta, LogicalPlan.StreamMeta> e : NodeMeta.this.outputStreams.entrySet()) {
            LogicalPlan.OutputPortMeta pm = e.getKey();
            if (pm.nodeMeta == NodeMeta.this && pm.fieldName.equals(field.getName())) {
              //LOG.debug("Found existing port meta for: " + field);
              outPortMap.put(portObject, pm);
              markOutputPortIfHidden(pm.getPortName(), pm, field.getDeclaringClass());
              return;
            }
          }
        }
        OutputPortMeta metaPort = new OutputPortMeta();
        metaPort.nodeMeta = NodeMeta.this;
        metaPort.fieldName = field.getName();
        metaPort.portAnnotation = portAnnotation;
        metaPort.adrAnnotation = adrAnnotation;
        outPortMap.put(portObject, metaPort);
        markOutputPortIfHidden(metaPort.getPortName(), metaPort, field.getDeclaringClass());
      }

      private void markOutputPortIfHidden(String portName, OutputPortMeta portMeta, Class<?> declaringClass)
      {
        if (!portNameMap.containsKey(portName)) {
          portNameMap.put(portName, portMeta);
        } else {
          // make the port optional
          portMeta.classDeclaringHiddenPort = declaringClass;
        }

      }

      private void markInputPortIfHidden(String portName, InputPortMeta portMeta, Class<?> declaringClass)
      {
        if (!portNameMap.containsKey(portName)) {
          portNameMap.put(portName, portMeta);
        } else {
          // make the port optional
          portMeta.classDeclaringHiddenPort = declaringClass;
        }
      }
    }
    /**
     * Ports objects are transient, we keep a lazy initialized mapping
     */
    private transient PortMapping portMapping = null;

    protected PortMapping getPortMapping()
    {
      if (this.portMapping == null) {
        this.portMapping = new PortMapping();
        Operators.describe(this.getComponent(), portMapping);
      }
      return portMapping;
    }

    public Map<InputPortMeta, StreamMeta> getInputStreams()
    {
      return this.inputStreams;
    }

    public Map<OutputPortMeta, StreamMeta> getOutputStreams()
    {
      return this.outputStreams;
    }

    public OperatorMeta getOperatorMeta() {
      if (this instanceof OperatorMeta)
        return (OperatorMeta)this;
      return null;
    }
  }

  /**
   * Operator meta object.
   */
  public final class OperatorMeta extends NodeMeta implements DAG.OperatorMeta, Serializable
  {
    private final Attribute.AttributeMap attributes;
    @SuppressWarnings("unused")
    private final int id;
    @NotNull
    private final String name;
    private final OperatorAnnotation operatorAnnotation;
    private final LogicalOperatorStatus status;
    private transient Integer nindex; // for cycle detection
    private transient Integer lowlink; // for cycle detection
    private MetricAggregatorMeta metricAggregatorMeta;
    private String moduleName;  // Name of the module which has this operator. null if this is a top level operator.

    /*
     * Used for  OIO validation,
     *  value null => node not visited yet
     *  other value => represents the root oio node for this node
     */
    private transient Integer oioRoot = null;

    private OperatorMeta(String name, Operator operator)
    {
      this(name, operator, new DefaultAttributeMap());
    }

    private OperatorMeta(String name, Operator operator, Attribute.AttributeMap attributeMap)
    {
      super(name);
      LOG.debug("Initializing {} as {}", name, operator.getClass().getName());
      this.operatorAnnotation = operator.getClass().getAnnotation(OperatorAnnotation.class);
      this.name = name;
      this.setComponent(operator);
      this.id = logicalOperatorSequencer.decrementAndGet();
      this.status = new LogicalOperatorStatus(name);
      this.attributes = attributeMap;
    }

    @Override
    public Attribute.AttributeMap getAttributes()
    {
      return attributes;
    }

    @Override
    public <T> T getValue(Attribute<T> key)
    {
      T attr = attributes.get(key);
      if (attr == null) {
        attr =  LogicalPlan.this.getValue(key);
      }
      if(attr == null){
        return key.defaultValue;
      }
      return attr;
    }

    public LogicalOperatorStatus getStatus()
    {
      return status;
    }

    private void writeObject(ObjectOutputStream out) throws IOException
    {
      //getValue2(OperatorContext.STORAGE_AGENT).save(operator, id, Checkpoint.STATELESS_CHECKPOINT_WINDOW_ID);
      out.defaultWriteObject();
      FSStorageAgent.store(out, component);
    }

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException
    {
      input.defaultReadObject();
      // TODO: not working because - we don't have the storage agent in parent attribuet map
      //operator = (Operator)getValue2(OperatorContext.STORAGE_AGENT).load(id, Checkpoint.STATELESS_CHECKPOINT_WINDOW_ID);
      component = (Operator)FSStorageAgent.retrieve(input);
    }

    @Override
    public void setCounters(Object counters)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void sendMetrics(Collection<String> metricNames)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public MetricAggregatorMeta getMetricAggregatorMeta()
    {
      return metricAggregatorMeta;
    }

    public String getModuleName()
    {
      return moduleName;
    }

    public void setModuleName(String moduleName)
    {
      this.moduleName = moduleName;
    }

    protected void populateAggregatorMeta()
    {
      AutoMetric.Aggregator aggregator = getValue(OperatorContext.METRICS_AGGREGATOR);
      if (aggregator == null) {
        MetricsAggregator defAggregator = null;
        Set<String> metricNames = Sets.newHashSet();

        for (Field field : ReflectionUtils.getDeclaredFieldsIncludingInherited(component.getClass())) {

          if (field.isAnnotationPresent(AutoMetric.class)) {
            metricNames.add(field.getName());

            if (field.getType() == int.class || field.getType() == Integer.class ||
              field.getType() == long.class || field.getType() == Long.class) {
              if (defAggregator == null) {
                defAggregator = new MetricsAggregator();
              }
              defAggregator.addAggregators(field.getName(), new SingleMetricAggregator[]{new LongSumAggregator()});
            }
            else if (field.getType() == float.class || field.getType() == Float.class ||
              field.getType() == double.class || field.getType() == Double.class) {
              if (defAggregator == null) {
                defAggregator = new MetricsAggregator();
              }
              defAggregator.addAggregators(field.getName(), new SingleMetricAggregator[]{new DoubleSumAggregator()});
            }
          }
        }

        try {
          for (PropertyDescriptor pd : Introspector.getBeanInfo(component.getClass()).getPropertyDescriptors()) {
            Method readMethod = pd.getReadMethod();
            if (readMethod != null) {
              AutoMetric rfa = readMethod.getAnnotation(AutoMetric.class);
              if (rfa != null) {
                String propName = pd.getName();
                if (metricNames.contains(propName)) {
                  continue;
                }

                if (readMethod.getReturnType() == int.class || readMethod.getReturnType() == Integer.class ||
                  readMethod.getReturnType() == long.class || readMethod.getReturnType() == Long.class) {

                  if (defAggregator == null) {
                    defAggregator = new MetricsAggregator();
                  }
                  defAggregator.addAggregators(propName, new SingleMetricAggregator[]{new LongSumAggregator()});

                } else if (readMethod.getReturnType() == float.class || readMethod.getReturnType() == Float.class ||
                  readMethod.getReturnType() == double.class || readMethod.getReturnType() == Double.class) {

                  if (defAggregator == null) {
                    defAggregator = new MetricsAggregator();
                  }
                  defAggregator.addAggregators(propName, new SingleMetricAggregator[]{new DoubleSumAggregator()});
                }
              }
            }
          }
        } catch (IntrospectionException e) {
          throw new RuntimeException("finding methods", e);
        }

        if (defAggregator != null) {
          aggregator = defAggregator;
        }
      }
      this.metricAggregatorMeta = new MetricAggregatorMeta(aggregator,
        getValue(OperatorContext.METRICS_DIMENSIONS_SCHEME));
    }

    private void copyAttributes(AttributeMap dest, AttributeMap source) {
      for (Entry<Attribute<?>, ?> a : source.entrySet())
        dest.put((Attribute<Object>)a.getKey(), a.getValue());
    }

    private void copyFrom(OperatorMeta operatorMeta)
    {
      // copy operator attributes
      copyAttributes(attributes, operatorMeta.getAttributes());

      // copy Input port attributes
      for (Map.Entry<InputPort<?>, InputPortMeta> entry : operatorMeta.getPortMapping().inPortMap.entrySet()) {
        InputPort<?> key = entry.getKey();
        InputPortMeta dest = getPortMapping().inPortMap.get(key);
        InputPortMeta source = entry.getValue();
        copyAttributes(dest.attributes, source.attributes);
      }

      // copy Output port attributes
      for (Map.Entry<OutputPort<?>, OutputPortMeta> entry : operatorMeta.getPortMapping().outPortMap.entrySet()) {
        OutputPort<?> key = entry.getKey();
        OutputPortMeta dest = getPortMapping().outPortMap.get(key);
        OutputPortMeta source = entry.getValue();
        copyAttributes(dest.attributes, source.attributes);
      }
    }

    @Override
    public OutputPortMeta getMeta(Operator.OutputPort<?> port)
    {
      return getPortMapping().outPortMap.get(port);
    }

    @Override
    public InputPortMeta getMeta(Operator.InputPort<?> port)
    {
      return getPortMapping().inPortMap.get(port);
    }

    @Override
    public Operator getOperator()
    {
      return (Operator)component;
    }

    public LogicalPlan getDAG()
    {
      return LogicalPlan.this;
    }

    @Override
    public String toString()
    {
      return "OperatorMeta{" + "name=" + name + ", operator=" + component + ", attributes=" + attributes + '}';
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof OperatorMeta)) {
        return false;
      }

      OperatorMeta that = (OperatorMeta) o;

      if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) {
        return false;
      }
      if (!name.equals(that.name)) {
        return false;
      }
      if (operatorAnnotation != null ? !operatorAnnotation.equals(that.operatorAnnotation) : that.operatorAnnotation != null) {
        return false;
      }
      return !(component != null ? !component.equals(that.component) : that.component != null);
    }

    @Override
    public int hashCode()
    {
      return name.hashCode();
    }

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201401091635L;
  }

  @Override
  public <T extends Operator> T addOperator(String name, Class<T> clazz)
  {
    T instance;
    try {
      instance = clazz.newInstance();
    } catch (Exception ex) {
      throw new IllegalArgumentException(ex);
    }
    addOperator(name, instance);
    return instance;
  }

  @Override
  public <T extends Operator> T addOperator(String name, T operator)
  {
    if (operators.containsKey(name)) {
      if (operators.get(name).component == operator) {
        return operator;
      }
      throw new IllegalArgumentException("duplicate operator id: " + operators.get(name));
    }

    // Avoid name conflict with module.
    if (modules.containsKey(name))
      throw new IllegalArgumentException("duplicate operator id: " + operators.get(name));

    OperatorMeta decl = new OperatorMeta(name, operator);
    rootOperators.add(decl); // will be removed when a sink is added to an input port for this operator
    operators.put(name, decl);
    return operator;
  }

  /**
   * Module meta object.
   */
  public final class ModuleMeta extends NodeMeta implements DAG.ModuleMeta, Serializable
  {
    private final Attribute.AttributeMap attributes;
    @NotNull
    private String name;
    private ModuleMeta parent;
    private LogicalPlan dag = null;
    private transient String fullName;
    private boolean expanded;

    private ModuleMeta(String name, Module module)
    {
      super(name);
      LOG.debug("Initializing {} as {}", name, module.getClass().getName());
      this.name = name;
      this.setComponent(module);
      this.attributes = new DefaultAttributeMap();
      this.dag = new LogicalPlan();
    }

    @Override
    public String getName()
    {
      return name;
    }

    @Override
    public Module getModule()
    {
      return (Module)component;
    }

    @Override
    public Attribute.AttributeMap getAttributes()
    {
      return attributes;
    }

    @Override
    public <T> T getValue(Attribute<T> key)
    {
      return attributes.get(key);
    }

    @Override
    public void setCounters(Object counters)
    {

    }

    @Override
    public void sendMetrics(Collection<String> metricNames)
    {

    }

    public LogicalPlan getDag()
    {
      return dag;
    }

    private void writeObject(ObjectOutputStream out) throws IOException
    {
      out.defaultWriteObject();
      FSStorageAgent.store(out, component);
    }

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException
    {
      input.defaultReadObject();
      component = (Module)FSStorageAgent.retrieve(input);
    }

    /**
     * Expand the module and add its operator to the parentDAG. After this method finishes the
     * module is expanded fully with all its submodules also expanded. The parentDAG contains
     * the operator added by all the modules.
     *
     * @param parentDAG parent dag to populate with operators from this and inner modules.
     * @param conf configuration object.
     */
    public void flattenModule(LogicalPlan parentDAG, Configuration conf)
    {
      if (dag.isExpanded())
        return;

      Module module = (Module)component;
      module.populateDAG(dag, conf);
      for (ModuleMeta subModuleMeta : dag.getAllModules()) {
        subModuleMeta.setParent(this);
        subModuleMeta.flattenModule(dag, conf);
      }
      dag.applyStreamLinks();
      parentDAG.addDAGToCurrentDAG(this);
      dag.setExpanded();
    }

    /**
     * Return full name of the module. If this is a inner module, i.e module inside of module this method will
     * traverse till the top level module, and construct the name by concatenating name of modules in the chain
     * in reverse order separated by MODULE_NAMESPACE_SEPARATO.
     *
     * For example If there is module M1, which adds another module M2 in the DAG. Then the full name of the
     * module M2 is ("M1" ++ MODULE_NAMESPACE_SEPARATO + "M2")
     *
     * @return full name of the module.
     */
    public String getFullName()
    {
      if (fullName != null)
        return fullName;

      if (parent == null) {
        fullName = name;
      } else {
        fullName = parent.getFullName() + MODULE_NAMESPACE_SEPARATOR + name;
      }
      return fullName;
    }

    private void setParent(ModuleMeta meta) {
      this.parent = meta;
    }

    private static final long serialVersionUID = 7562277769188329223L;
  }

  @Override
  public <T extends Module> T addModule(String name, T module)
  {
    if (modules.containsKey(name)) {
      if (modules.get(name).component == module) {
        return module;
      }
      throw new IllegalArgumentException("duplicate module is: " + modules.get(name));
    }
    if (operators.containsKey(name))
      throw new IllegalArgumentException("duplicate module is: " + modules.get(name));

    ModuleMeta meta = new ModuleMeta(name, module);
    modules.put(name, meta);
    return module;
  }

  @Override
  public <T extends Module> T addModule(String name, Class<T> clazz)
  {
    T instance;
    try {
      instance = clazz.newInstance();
    } catch (Exception ex) {
      throw new IllegalArgumentException(ex);
    }
    addModule(name, instance);
    return instance;
  }

  public void removeOperator(Operator operator)
  {
    OperatorMeta om = getMeta(operator);
    if (om == null) {
      return;
    }

    Map<InputPortMeta, StreamMeta> inputStreams = om.getInputStreams();
    for (Map.Entry<InputPortMeta, StreamMeta> e : inputStreams.entrySet()) {
      StreamMeta stream = e.getValue();
      if (e.getKey().getNodeMeta() == om) {
         stream.sinks.remove(e.getKey());
      }
      // If persistStream was enabled for stream, reset stream when sink removed
      stream.resetStreamPersistanceOnSinkRemoval(e.getKey());
    }
    this.operators.remove(om.getName());
    rootOperators.remove(om);
  }

  @Override
  public StreamMeta addStream(String id)
  {
    StreamMeta s = new StreamMeta(id);
    StreamMeta o = streams.put(id, s);
    if (o == null) {
      return s;
    }

    throw new IllegalArgumentException("duplicate stream id: " + o);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T>... sinks)
  {
    StreamMeta s = addStream(id);
    id = s.id;
    ArrayListMultimap<Operator.OutputPort<?>, Operator.InputPort<?>> streamMap = ArrayListMultimap.create();
    if (!(source instanceof ProxyOutputPort)) {
      s.setSource(source);
    }
    for (Operator.InputPort<?> sink : sinks) {
      if (source instanceof ProxyOutputPort || sink instanceof ProxyInputPort) {
        streamMap.put(source, sink);
        streamLinks.put(id, streamMap);
      } else {
        if (s.getSource() == null) {
          s.setSource(source);
        }
        s.addSink(sink);
      }
    }
    return s;
  }

  /**
   * This will be called once the Logical Dag is expanded, and the proxy input and proxy output ports are populated with the actual ports that they refer to
   * This method adds sources and sinks for the StreamMeta objects which were left empty in the addStream call.
   */
  public void applyStreamLinks()
  {
    for (String id : streamLinks.keySet()) {
      StreamMeta s = getStream(id);
      for (Map.Entry<Operator.OutputPort<?>, Operator.InputPort<?>> pair : streamLinks.get(id).entries()) {
        if (s.getSource() == null || s.getSource().getPortObject() == null || s.getSource().getPortObject() instanceof ProxyOutputPort) {
          Operator.OutputPort<?> outputPort = pair.getKey();
          while (outputPort instanceof ProxyOutputPort) {
            outputPort = ((ProxyOutputPort<?>)outputPort).get();
          }
          s.setSource(outputPort);
        }

        Operator.InputPort<?> inputPort = pair.getValue();
        while (inputPort instanceof ProxyInputPort) {
          inputPort = ((ProxyInputPort<?>)inputPort).get();
        }
        s.addSink(inputPort);
      }
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void addDAGToCurrentDAG(ModuleMeta moduleMeta)
  {
    LogicalPlan subDag = moduleMeta.getDag();
    String subDAGName = moduleMeta.getName();
    String name;
    for (OperatorMeta operatorMeta : subDag.getAllOperators()) {
      name = subDAGName + MODULE_NAMESPACE_SEPARATOR + operatorMeta.getName();
      Operator op = this.addOperator(name, operatorMeta.getOperator());
      OperatorMeta ometa = this.getMeta(op);
      ometa.copyFrom(operatorMeta);
      OperatorMeta operatorMetaNew = this.getOperatorMeta(name);
      operatorMetaNew.setModuleName(operatorMeta.getModuleName() == null ? subDAGName : subDAGName + MODULE_NAMESPACE_SEPARATOR + operatorMeta.getModuleName());
    }

    for (StreamMeta streamMeta : subDag.getAllStreams()) {
      OutputPortMeta sourceMeta = streamMeta.getSource();
      List<InputPort<?>> ports = new LinkedList<>();
      for (InputPortMeta inputPortMeta : streamMeta.getSinks()) {
        ports.add(inputPortMeta.getPortObject());
      }
      InputPort[] inputPorts = ports.toArray(new InputPort[]{});

      name = subDAGName + MODULE_NAMESPACE_SEPARATOR + streamMeta.getName();
      StreamMeta streamMetaNew = this.addStream(name, sourceMeta.getPortObject(), inputPorts);
      streamMetaNew.setModuleName(streamMeta.getModuleName() == null ? subDAGName : subDAGName + "_" + streamMeta.getModuleName());
      streamMetaNew.copyFrom(streamMeta);
    }

  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1)
  {
    @SuppressWarnings("rawtypes")
    InputPort[] ports = new Operator.InputPort[]{sink1};
    return addStream(id, source, ports);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1, Operator.InputPort<? super T> sink2)
  {
    @SuppressWarnings("rawtypes")
    InputPort[] ports = new Operator.InputPort[] {sink1, sink2};
    return addStream(id, source, ports);
  }

  public StreamMeta getStream(String id)
  {
    return this.streams.get(id);
  }

  /**
   * Set attribute for the operator. For valid attributes, see {
   *
   * @param operator
   * @return AttributeMap
   */
  public Attribute.AttributeMap getContextAttributes(Operator operator)
  {
    return getMeta(operator).attributes;
  }

  @Override
  public <T> void setAttribute(Attribute<T> key, T value)
  {
    this.getAttributes().put(key, value);
  }

  @Override
  public <T> void setAttribute(Operator operator, Attribute<T> key, T value)
  {
    this.getMeta(operator).attributes.put(key, value);
  }

  private OutputPortMeta assertGetPortMeta(Operator.OutputPort<?> port)
  {
    for (OperatorMeta o : getAllOperators()) {
      OutputPortMeta opm = o.getPortMapping().outPortMap.get(port);
      if (opm != null) {
        return opm;
      }
    }

    for(ModuleMeta m : getAllModules()) {
      OutputPortMeta opm = m.getPortMapping().outPortMap.get(port);
      if (opm != null)
        return opm;
    }

    throw new IllegalArgumentException("Port is not associated to any operator in the DAG: " + port);
  }

  private InputPortMeta assertGetPortMeta(Operator.InputPort<?> port)
  {
    for (OperatorMeta o : getAllOperators()) {
      InputPortMeta ipm = o.getPortMapping().inPortMap.get(port);
      if (ipm != null) {
        return ipm;
      }
    }

    for(ModuleMeta m : getAllModules()) {
      System.out.println("getting port name for m " + m.getName());
      InputPortMeta ipm = m.getPortMapping().inPortMap.get(port);
      if (ipm != null)
        return ipm;
    }

    throw new IllegalArgumentException("Port is not associated to any operator in the DAG: " + port);
  }

  @Override
  public <T> void setOutputPortAttribute(Operator.OutputPort<?> port, Attribute<T> key, T value)
  {
    assertGetPortMeta(port).attributes.put(key, value);
  }

  @Override
  public <T> void setUnifierAttribute(Operator.OutputPort<?> port, Attribute<T> key, T value)
  {
    assertGetPortMeta(port).getUnifierMeta().attributes.put(key, value);
  }

  @Override
  public <T> void setInputPortAttribute(Operator.InputPort<?> port, Attribute<T> key, T value)
  {
    assertGetPortMeta(port).attributes.put(key, value);
  }

  public List<OperatorMeta> getRootOperators()
  {
    List<OperatorMeta> operators = new ArrayList<>();
    for(NodeMeta item : this.rootOperators) {
      operators.add((OperatorMeta)item);
    }
    return Collections.unmodifiableList(operators);
  }

  public Collection<OperatorMeta> getAllOperators()
  {
    return Collections.unmodifiableCollection(this.operators.values());
  }

  public Collection<ModuleMeta> getAllModules()
  {
    return Collections.unmodifiableCollection(this.modules.values());
  }

  public Collection<StreamMeta> getAllStreams()
  {
    return Collections.unmodifiableCollection(this.streams.values());
  }

  @Override
  public OperatorMeta getOperatorMeta(String operatorName)
  {
    return this.operators.get(operatorName);
  }

  public ModuleMeta getModuleMeta(String moduleName)
  {
    return this.modules.get(moduleName);
  }

  @Override
  public OperatorMeta getMeta(Operator operator)
  {
    // TODO: cache mapping
    for (OperatorMeta o: getAllOperators()) {
      if (o.component == operator) {
        return o;
      }
    }
    throw new IllegalArgumentException("Operator not associated with the DAG: " + operator);
  }

  public ModuleMeta getMeta(Module module)
  {
    for (ModuleMeta m : getAllModules()) {
      if (m.component == module) {
        return m;
      }
    }
    throw new IllegalArgumentException("Module not associated with the DAG: " + module);
  }

  public int getMaxContainerCount()
  {
    return this.getValue(CONTAINERS_MAX_COUNT);
  }

  public boolean isDebug()
  {
    return this.getValue(DEBUG);
  }

  public int getMasterMemoryMB()
  {
    return this.getValue(MASTER_MEMORY_MB);
  }

  public String getMasterJVMOptions()
  {
    return this.getValue(CONTAINER_JVM_OPTIONS);
  }

  public String assertAppPath()
  {
    String path = getAttributes().get(LogicalPlan.APPLICATION_PATH);
    if (path == null) {
      throw new AssertionError("Missing " + LogicalPlan.APPLICATION_PATH);
    }
    return path;
  }

  /**
   * Class dependencies for the topology. Used to determine jar file dependencies.
   *
   * @return Set<String>
   */
  public Set<String> getClassNames()
  {
    Set<String> classNames = new HashSet<String>();
    for (OperatorMeta n: this.operators.values()) {
      String className = n.getOperator().getClass().getName();
      if (className != null) {
        classNames.add(className);
      }
    }
    for (StreamMeta n: this.streams.values()) {
      for (InputPortMeta sink : n.getSinks()) {
        StreamCodec<?> streamCodec = (StreamCodec<?>)sink.getValue(PortContext.STREAM_CODEC);
        if (streamCodec != null) {
          classNames.add(streamCodec.getClass().getName());
        } else {
          StreamCodec<?> codec = sink.getPortObject().getStreamCodec();
          if (codec != null) {
            classNames.add(codec.getClass().getName());
          }
        }
      }
    }
    return classNames;
  }

  /**
   * Validate the plan. Includes checks that required ports are connected,
   * required configuration parameters specified, graph free of cycles etc.
   *
   * @throws ConstraintViolationException
   */
  public void validate() throws ConstraintViolationException
  {
    ValidatorFactory factory =
            Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();

    checkAttributeValueSerializable(this.getAttributes(), DAG.class.getName());

    // clear oioRoot values in all operators
    for (OperatorMeta n: operators.values()) {
      n.oioRoot = null;
    }

    // clear visited on all operators
    for (OperatorMeta n: operators.values()) {
      n.nindex = null;
      n.lowlink = null;

      // validate configuration
      Set<ConstraintViolation<Operator>> constraintViolations = validator.validate(n.getOperator());
      if (!constraintViolations.isEmpty()) {
        Set<ConstraintViolation<?>> copySet = new HashSet<ConstraintViolation<?>>(constraintViolations.size());
        // workaround bug in ConstraintViolationException constructor
        // (should be public <T> ConstraintViolationException(String message, Set<ConstraintViolation<T>> constraintViolations) { ... })
        for (ConstraintViolation<Operator> cv: constraintViolations) {
          copySet.add(cv);
        }
        throw new ConstraintViolationException("Operator " + n.getName() + " violates constraints " + copySet, copySet);
      }

      NodeMeta.PortMapping portMapping = n.getPortMapping();

      checkAttributeValueSerializable(n.getAttributes(), n.getName());

      // Check operator annotation
      if (n.operatorAnnotation != null) {
        // Check if partition property of the operator is being honored
        if (!n.operatorAnnotation.partitionable()) {
          // Check if any of the input ports have partition attributes set
          for (InputPortMeta pm: portMapping.inPortMap.values()) {
            Boolean paralellPartition = (Boolean)pm.getValue(PortContext.PARTITION_PARALLEL);
            if (paralellPartition) {
              throw new ValidationException("Operator " + n.getName() + " is not partitionable but PARTITION_PARALLEL attribute is set");
            }
          }

          // Check if the operator implements Partitioner
          if (n.getValue(OperatorContext.PARTITIONER) != null
              || n.attributes != null && !n.attributes.contains(OperatorContext.PARTITIONER) && Partitioner.class.isAssignableFrom(n.getOperator().getClass())) {
            throw new ValidationException("Operator " + n.getName() + " provides partitioning capabilities but the annotation on the operator class declares it non partitionable!");
          }
        }

        //If operator can not be check-pointed in middle of application window then the checkpoint window count should be
        // a multiple of application window count
        if (!n.operatorAnnotation.checkpointableWithinAppWindow()) {
          if (n.getValue(OperatorContext.CHECKPOINT_WINDOW_COUNT) % n.getValue(OperatorContext.APPLICATION_WINDOW_COUNT) != 0) {
            throw new ValidationException("Operator " + n.getName() + " cannot be check-pointed between an application window " +
              "but the checkpoint-window-count " + n.getValue(OperatorContext.CHECKPOINT_WINDOW_COUNT) +
              " is not a multiple application-window-count " + n.getValue(OperatorContext.APPLICATION_WINDOW_COUNT));
          }
        }
      }

      // check that non-optional ports are connected
      for (InputPortMeta pm: portMapping.inPortMap.values()) {
        checkAttributeValueSerializable(pm.getAttributes(), n.getName() + "." + pm.getPortName());
        StreamMeta sm = n.inputStreams.get(pm);
        if (sm == null) {
          if ((pm.portAnnotation == null || !pm.portAnnotation.optional()) && pm.classDeclaringHiddenPort == null) {
            throw new ValidationException("Input port connection required: " + n.name + "." + pm.getPortName());
          }
        } else {
          if (pm.classDeclaringHiddenPort != null) {
            throw new ValidationException(String.format("Invalid port connected: %s.%s is hidden by %s.%s", pm.classDeclaringHiddenPort.getName(),
              pm.getPortName(), pm.nodeMeta.getComponent().getClass().getName(), pm.getPortName()));
          }
          // check locality constraints
          DAG.Locality locality = sm.getLocality();
          if (locality == DAG.Locality.THREAD_LOCAL) {
            if (n.inputStreams.size() > 1) {
              validateThreadLocal(n);
            }
          }

          if (pm.portAnnotation != null && pm.portAnnotation.schemaRequired()) {
            //since schema is required, the port attribute TUPLE_CLASS should be present
            if (pm.attributes.get(PortContext.TUPLE_CLASS) == null) {
              throw new ValidationException("Attribute " + PortContext.TUPLE_CLASS.getName() + " missing on port : " + n.name + "." + pm.getPortName());
            }
          }
        }
      }

      boolean allPortsOptional = true;
      for (OutputPortMeta pm: portMapping.outPortMap.values()) {
        checkAttributeValueSerializable(pm.getAttributes(), n.getName() + "." + pm.getPortName());
        if (!n.outputStreams.containsKey(pm)) {
          if ((pm.portAnnotation != null && !pm.portAnnotation.optional()) && pm.classDeclaringHiddenPort == null) {
            throw new ValidationException("Output port connection required: " + n.name + "." + pm.getPortName());
          }
        } else {
          //port is connected
          if (pm.classDeclaringHiddenPort != null) {
            throw new ValidationException(String.format("Invalid port connected: %s.%s is hidden by %s.%s", pm.classDeclaringHiddenPort.getName(),
              pm.getPortName(), pm.nodeMeta.getComponent().getClass().getName(), pm.getPortName()));
          }
          if (pm.portAnnotation != null && pm.portAnnotation.schemaRequired()) {
            //since schema is required, the port attribute TUPLE_CLASS should be present
            if (pm.attributes.get(PortContext.TUPLE_CLASS) == null) {
              throw new ValidationException("Attribute " + PortContext.TUPLE_CLASS.getName() + " missing on port : " + n.name + "." + pm.getPortName());
            }
          }
        }
        allPortsOptional &= (pm.portAnnotation != null && pm.portAnnotation.optional());
      }
      if (!allPortsOptional && n.outputStreams.isEmpty()) {
        throw new ValidationException("At least one output port must be connected: " + n.name);
      }
    }
    stack = new Stack<OperatorMeta>();

    List<List<String>> cycles = new ArrayList<List<String>>();
    for (OperatorMeta n: operators.values()) {
      if (n.nindex == null) {
        findStronglyConnected(n, cycles);
      }
    }
    if (!cycles.isEmpty()) {
      throw new ValidationException("Loops in graph: " + cycles);
    }

    for (StreamMeta s: streams.values()) {
      if (s.source == null) {
        throw new ValidationException("Stream source not connected: " + s.getName());
      }

      if (s.sinks.isEmpty()) {
        throw new ValidationException("Stream sink not connected: " + s.getName());
      }
    }

    // Validate root operators are input operators
    for (NodeMeta om : this.rootOperators) {
      if (!(om.getComponent() instanceof InputOperator)) {
        throw new ValidationException(String.format("Root operator: %s is not a Input operator",
            om.getName()));
      }
    }

    // processing mode
    Set<OperatorMeta> visited = Sets.newHashSet();
    for (NodeMeta om : this.rootOperators) {
      validateProcessingMode((OperatorMeta)om, visited);
    }

  }

  private void checkAttributeValueSerializable(AttributeMap attributes, String context)
  {
    StringBuilder sb = new StringBuilder();
    String delim = "";
    // Check all attributes got operator are serializable
    for (Entry<Attribute<?>, Object> entry : attributes.entrySet()) {
      if (entry.getValue() != null && !(entry.getValue() instanceof Serializable)) {
        sb.append(delim).append(entry.getKey().getSimpleName());
        delim = ", ";
      }
    }
    if (sb.length() > 0) {
      throw new ValidationException("Attribute value(s) for " + sb.toString() + " in " + context + " are not serializable");
    }
  }

  /*
   * Validates OIO constraints for operators with more than one input streams
   * For a node to be OIO,
   *  1. all its input streams should be OIO
   *  2. all its input streams should have OIO from single source node
   */
  private void validateThreadLocal(OperatorMeta om) {
    Integer oioRoot = null;

    // already visited and validated
    if (om.oioRoot != null) {
      return;
    }

    for (StreamMeta sm: om.inputStreams.values()){
      // validation fail as each input stream should be OIO
      if (sm.locality != Locality.THREAD_LOCAL){
        String msg = String.format("Locality %s invalid for operator %s with multiple input streams as at least one of the input streams is not %s",
                                   Locality.THREAD_LOCAL, om, Locality.THREAD_LOCAL);
        throw new ValidationException(msg);
      }

      // gets oio root for input operator for the stream
      Integer oioStreamRoot = getOioRoot((OperatorMeta)sm.source.nodeMeta);

      // validation fail as each input stream should have a common OIO root
      if (om.oioRoot != null && oioStreamRoot != om.oioRoot){
        String msg = String.format("Locality %s invalid for operator %s with multiple input streams as at least one of the input streams is not originating from common OIO owner node",
                                   Locality.THREAD_LOCAL, om, Locality.THREAD_LOCAL);
        throw new ValidationException(msg);
      }

      // populate oioRoot with root OIO node id for first stream, then validate for subsequent streams to have same root OIO node
      if (oioRoot == null) {
        oioRoot = oioStreamRoot;
      } else if (oioRoot.intValue() != oioStreamRoot.intValue()) {
        String msg = String.format("Locality %s invalid for operator %s with multiple input streams as they origin from different owner OIO operators", sm.locality, om);
        throw new ValidationException(msg);
      }
    }

    om.oioRoot = oioRoot;
  }

  /**
   * Helper method for validateThreadLocal method, runs recursively
   * For a given operator, visits all upstream operators in DFS, validates and marks them as visited
   * returns hashcode of owner oio node if it exists, else returns hashcode of the supplied node
   */
  private Integer getOioRoot(OperatorMeta om) {
    // operators which were already marked a visited
    if (om.oioRoot != null){
      return om.oioRoot;
    }

    // operators which were not visited before
    switch (om.inputStreams.size()) {
      case 1:
        StreamMeta sm = om.inputStreams.values().iterator().next();
        if (sm.locality == Locality.THREAD_LOCAL) {
          om.oioRoot = getOioRoot((OperatorMeta)sm.source.nodeMeta);
        }
        else {
          om.oioRoot = om.hashCode();
        }
        break;
      case 0:
        om.oioRoot = om.hashCode();
        break;
      default:
        validateThreadLocal(om);
    }

    return om.oioRoot;
  }

  /**
   * Check for cycles in the graph reachable from start node n. This is done by
   * attempting to find strongly connected components.
   *
   * @see <a href="http://en.wikipedia.org/wiki/Tarjan%E2%80%99s_strongly_connected_components_algorithm">http://en.wikipedia.org/wiki/Tarjan%E2%80%99s_strongly_connected_components_algorithm</a>
   *
   * @param om
   * @param cycles
   */
  public void findStronglyConnected(NodeMeta om, List<List<String>> cycles)
  {
    om.nindex = nodeIndex;
    om.lowlink = nodeIndex;
    nodeIndex++;
    stack.push((OperatorMeta)om);

    // depth first successors traversal
    for (StreamMeta downStream: om.outputStreams.values()) {
      for (InputPortMeta sink: downStream.sinks) {
        NodeMeta successor = sink.getNodeMeta();
        if (successor == null) {
          continue;
        }
        // check for self referencing node
        if (om == successor) {
          cycles.add(Collections.singletonList(om.name));
        }
        if (successor.nindex == null) {
          // not visited yet
          findStronglyConnected(successor, cycles);
          om.lowlink = Math.min(om.lowlink, successor.lowlink);
        }
        else if (stack.contains(successor)) {
          om.lowlink = Math.min(om.lowlink, successor.nindex);
        }
      }
    }

    // pop stack for all root operators
    if (om.lowlink.equals(om.nindex)) {
      List<String> connectedIds = new ArrayList<String>();
      while (!stack.isEmpty()) {
        NodeMeta n2 = stack.pop();
        connectedIds.add(n2.name);
        if (n2 == om) {
          break; // collected all connected operators
        }
      }
      // strongly connected (cycle) if more than one node in stack
      if (connectedIds.size() > 1) {
        LOG.debug("detected cycle from node {}: {}", om.name, connectedIds);
        cycles.add(connectedIds);
      }
    }
  }

  private void validateProcessingMode(OperatorMeta om, Set<OperatorMeta> visited)
  {
    for (StreamMeta is : om.getInputStreams().values()) {
      if (!visited.contains(is.getSource().getNodeMeta())) {
        // process all inputs first
        return;
      }
    }
    visited.add(om);
    Operator.ProcessingMode pm = om.getValue(OperatorContext.PROCESSING_MODE);
    for (StreamMeta os : om.outputStreams.values()) {
      for (InputPortMeta sink: os.sinks) {
        OperatorMeta sinkOm = (OperatorMeta)sink.getNodeMeta();
        Operator.ProcessingMode sinkPm = sinkOm.attributes == null? null: sinkOm.attributes.get(OperatorContext.PROCESSING_MODE);
        if (sinkPm == null) {
          // If the source processing mode is AT_MOST_ONCE and a processing mode is not specified for the sink then set it to AT_MOST_ONCE as well
          if (Operator.ProcessingMode.AT_MOST_ONCE.equals(pm)) {
            LOG.warn("Setting processing mode for operator {} to {}", sinkOm.getName(), pm);
            sinkOm.getAttributes().put(OperatorContext.PROCESSING_MODE, pm);
          } else if (Operator.ProcessingMode.EXACTLY_ONCE.equals(pm)) {
            // If the source processing mode is EXACTLY_ONCE and a processing mode is not specified for the sink then throw a validation error
            String msg = String.format("Processing mode for %s should be AT_MOST_ONCE for source %s/%s", sinkOm.getName(), om.getName(), pm);
            throw new ValidationException(msg);
          }
        } else {
          /*
           * If the source processing mode is AT_MOST_ONCE and the processing mode for the sink is not AT_MOST_ONCE throw a validation error
           * If the source processing mode is EXACTLY_ONCE and the processing mode for the sink is not AT_MOST_ONCE throw a validation error
           */
          if ((Operator.ProcessingMode.AT_MOST_ONCE.equals(pm) && (sinkPm != pm))
                  || (Operator.ProcessingMode.EXACTLY_ONCE.equals(pm) && !Operator.ProcessingMode.AT_MOST_ONCE.equals(sinkPm))) {
            String msg = String.format("Processing mode %s/%s not valid for source %s/%s", sinkOm.getName(), sinkPm, om.getName(), pm);
            throw new ValidationException(msg);
          }
        }
        validateProcessingMode(sinkOm, visited);
      }
    }
  }

  public static void write(DAG dag, OutputStream os) throws IOException
  {
    ObjectOutputStream oos = new ObjectOutputStream(os);
    oos.writeObject(dag);
  }

  public static LogicalPlan read(InputStream is) throws IOException, ClassNotFoundException
  {
    return (LogicalPlan)new ObjectInputStream(is).readObject();
  }


  public static Type getPortType(Field f)
  {
    if (f.getGenericType() instanceof ParameterizedType) {
      ParameterizedType t = (ParameterizedType)f.getGenericType();
      //LOG.debug("Field type is parameterized: " + Arrays.asList(t.getActualTypeArguments()));
      //LOG.debug("rawType: " + t.getRawType()); // the port class
      Type typeArgument = t.getActualTypeArguments()[0];
      if (typeArgument instanceof Class) {
        return typeArgument;
      }
      else if (typeArgument instanceof TypeVariable) {
        TypeVariable<?> tv = (TypeVariable<?>)typeArgument;
        LOG.debug("bounds: " + Arrays.asList(tv.getBounds()));
        // variable may contain other variables, java.util.Map<java.lang.String, ? extends T2>
        return tv.getBounds()[0];
      }
      else if (typeArgument instanceof GenericArrayType) {
        LOG.debug("type {} is of GenericArrayType", typeArgument);
        return typeArgument;
      }
      else if (typeArgument instanceof WildcardType) {
        LOG.debug("type {} is of WildcardType", typeArgument);
        return typeArgument;
      }
      else if (typeArgument instanceof ParameterizedType) {
        return typeArgument;
      }
      else {
        LOG.error("Type argument is of expected type {}", typeArgument);
        return null;
      }
    }
    else {
      // ports are always parameterized
      LOG.error("No type variable: {}, typeParameters: {}", f.getType(), Arrays.asList(f.getClass().getTypeParameters()));
      return null;
    }
  }

  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
            append("operators", this.operators).
            append("streams", this.streams).
            append("properties", this.attributes).
            toString();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LogicalPlan)) {
      return false;
    }

    LogicalPlan that = (LogicalPlan) o;

    if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) {
      return false;
    }
    return !(streams != null ? !streams.equals(that.streams) : that.streams != null);
  }

  @Override
  public int hashCode()
  {
    int result = streams != null ? streams.hashCode() : 0;
    result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
    return result;
  }

  public final class MetricAggregatorMeta implements Serializable
  {
    private final AutoMetric.Aggregator aggregator;
    private final AutoMetric.DimensionsScheme dimensionsScheme;

    protected MetricAggregatorMeta(AutoMetric.Aggregator aggregator,
                                   AutoMetric.DimensionsScheme dimensionsScheme)
    {
      this.aggregator = aggregator;
      this.dimensionsScheme = dimensionsScheme;
    }

    public AutoMetric.Aggregator getAggregator()
    {
      return this.aggregator;
    }

    public String[] getDimensionAggregatorsFor(String logicalMetricName)
    {
      if (dimensionsScheme == null) {
        return null;
      }
      return dimensionsScheme.getDimensionAggregationsFor(logicalMetricName);
    }

    public String[] getTimeBuckets()
    {
      if (dimensionsScheme == null) {
        return null;
      }
      return dimensionsScheme.getTimeBuckets();
    }

    private static final long serialVersionUID = 201604271719L;
  }

  public boolean isExpanded() {
    return expanded;
  }

  public void setExpanded() {
    expanded = true;
  }
}
