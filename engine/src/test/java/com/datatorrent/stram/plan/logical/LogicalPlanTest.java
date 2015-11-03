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

import com.datatorrent.common.util.BaseOperator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;

import javax.validation.*;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.api.*;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.engine.TestNonOptionalOutportInputOperator;
import com.datatorrent.stram.engine.TestOutputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.support.StramTestSupport.MemoryStorageAgent;
import com.datatorrent.stram.support.StramTestSupport.RegexMatcher;

public class LogicalPlanTest {

  @Test
  public void testCycleDetection() {
     LogicalPlan dag = new LogicalPlan();

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
     StreamMeta n7n7 = dag.addStream("n7n7", operator7.outport1, operator7.inport1);
     try {
       n7n7.addSink(operator7.inport1);
       fail("cannot add to stream again");
     } catch (Exception e) {
       // expected, stream can have single input/output only
     }

     List<List<String>> cycles = new ArrayList<List<String>>();
     dag.findStronglyConnected(dag.getMeta(operator7), cycles);
     assertEquals("operator self reference", 1, cycles.size());
     assertEquals("operator self reference", 1, cycles.get(0).size());
     assertEquals("operator self reference", dag.getMeta(operator7).getName(), cycles.get(0).get(0));

     // 3 operator cycle
     cycles.clear();
     dag.findStronglyConnected(dag.getMeta(operator4), cycles);
     assertEquals("3 operator cycle", 1, cycles.size());
     assertEquals("3 operator cycle", 3, cycles.get(0).size());
     assertTrue("operator2", cycles.get(0).contains(dag.getMeta(operator2).getName()));
     assertTrue("operator3", cycles.get(0).contains(dag.getMeta(operator3).getName()));
     assertTrue("operator4", cycles.get(0).contains(dag.getMeta(operator4).getName()));

     try {
       dag.validate();
       fail("validation should fail");
     } catch (ValidationException e) {
       // expected
     }

  }

  public static class ValidationOperator extends BaseOperator {
    public final transient DefaultOutputPort<Object> goodOutputPort = new DefaultOutputPort<Object>();

    public final transient DefaultOutputPort<Object> badOutputPort = new DefaultOutputPort<Object>();
  }

  public static class CounterOperator extends BaseOperator {
    final public transient InputPort<Object> countInputPort = new DefaultInputPort<Object>() {
      @Override
      final public void process(Object payload) {
      }
    };
  }

  @Test
  public void testLogicalPlanSerialization() throws Exception {

    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    ValidationOperator validationNode = dag.addOperator("validationNode", ValidationOperator.class);
    CounterOperator countGoodNode = dag.addOperator("countGoodNode", CounterOperator.class);
    CounterOperator countBadNode = dag.addOperator("countBadNode", CounterOperator.class);
    //ConsoleOutputOperator echoBadNode = dag.addOperator("echoBadNode", ConsoleOutputOperator.class);

    // good tuples to counter operator
    dag.addStream("goodTuplesStream", validationNode.goodOutputPort, countGoodNode.countInputPort);

    // bad tuples to separate stream and echo operator
    // (stream with 2 outputs)
    dag.addStream("badTuplesStream", validationNode.badOutputPort, countBadNode.countInputPort);

    Assert.assertEquals("number root operators", 1, dag.getRootOperators().size());
    Assert.assertEquals("root operator id", "validationNode", dag.getRootOperators().get(0).getName());

    dag.getContextAttributes(countGoodNode).put(OperatorContext.SPIN_MILLIS, 10);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    LogicalPlan.write(dag, bos);

    // System.out.println("serialized size: " + bos.toByteArray().length);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    LogicalPlan dagClone = LogicalPlan.read(bis);
    Assert.assertNotNull(dagClone);
    Assert.assertEquals("number operators in clone", dag.getAllOperators().size(), dagClone.getAllOperators().size());
    Assert.assertEquals("number root operators in clone", 1, dagClone.getRootOperators().size());
    Assert.assertTrue("root operator in operators", dagClone.getAllOperators().contains(dagClone.getRootOperators().get(0)));


    Operator countGoodNodeClone = dagClone.getOperatorMeta("countGoodNode").getOperator();
    Assert.assertEquals("", new Integer(10), dagClone.getContextAttributes(countGoodNodeClone).get(OperatorContext.SPIN_MILLIS));

  }

  @Test
  public void testDeleteOperator()
  {
    LogicalPlan dag = new LogicalPlan();
    TestGeneratorInputOperator input = dag.addOperator("input1", TestGeneratorInputOperator.class);
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.addStream("s0", input.outport, o1.inport1);
    StreamMeta s1 = dag.addStream("s1", o1.outport1, o2.inport1);
    dag.validate();
    Assert.assertEquals("", 3, dag.getAllOperators().size());

    dag.removeOperator(o2);
    s1.remove();
    dag.validate();
    Assert.assertEquals("", 2, dag.getAllOperators().size());
  }

  public static class ValidationTestOperator extends BaseOperator implements InputOperator {
    @NotNull
    @Pattern(regexp=".*malhar.*", message="Value has to contain 'malhar'!")
    private String stringField1;

    @Min(2)
    private int intField1;

    @AssertTrue(message="stringField1 should end with intField1")
    private boolean isValidConfiguration() {
      return stringField1.endsWith(String.valueOf(intField1));
    }

    private String getterProperty2 = "";

    @NotNull
    public String getProperty2() {
      return getterProperty2;
    }

    public void setProperty2(String s) {
      // annotations need to be on the getter
      getterProperty2 = s;
    }

    private String[] stringArrayField;

    public String[] getStringArrayField() {
      return stringArrayField;
    }

    public void setStringArrayField(String[] stringArrayField) {
      this.stringArrayField = stringArrayField;
    }

    public class Nested {
      @NotNull
      private String property = "";

      public String getProperty() {
        return property;
      }

      public void setProperty(String property) {
        this.property = property;
      }

    }

    @Valid
    private final Nested nestedBean = new Nested();

    private String stringProperty2;

    public String getStringProperty2() {
      return stringProperty2;
    }

    public void setStringProperty2(String stringProperty2) {
      this.stringProperty2 = stringProperty2;
    }

    private Map<String, String> mapProperty = Maps.newHashMap();

    public Map<String, String> getMapProperty()
    {
      return mapProperty;
    }

    public void setMapProperty(Map<String, String> mapProperty)
    {
      this.mapProperty = mapProperty;
    }

    @Override
    public void emitTuples() {
      // Emit no tuples

    }

  }

  @Test
  public void testOperatorValidation() {

    ValidationTestOperator bean = new ValidationTestOperator();
    bean.stringField1 = "malhar1";
    bean.intField1 = 1;

    // ensure validation standalone produces expected results
    ValidatorFactory factory =
        Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();
    Set<ConstraintViolation<ValidationTestOperator>> constraintViolations =
             validator.validate(bean);
    //for (ConstraintViolation<ValidationTestOperator> cv : constraintViolations) {
    //  System.out.println("validation error: " + cv);
    //}
    Assert.assertEquals("" + constraintViolations,1, constraintViolations.size());
    ConstraintViolation<ValidationTestOperator> cv = constraintViolations.iterator().next();
    Assert.assertEquals("", bean.intField1, cv.getInvalidValue());
    Assert.assertEquals("", "intField1", cv.getPropertyPath().toString());

    // ensure DAG validation produces matching results
    LogicalPlan dag = new LogicalPlan();
    bean = dag.addOperator("testOperator", bean);

    try {
      dag.validate();
      Assert.fail("should throw ConstraintViolationException");
    } catch (ConstraintViolationException e) {
      Assert.assertEquals("violation details", constraintViolations, e.getConstraintViolations());
      String expRegex = ".*ValidationTestOperator\\{name=null}, propertyPath='intField1', message='must be greater than or equal to 2',.*value=1}]";
      Assert.assertThat("exception message", e.getMessage(), RegexMatcher.matches(expRegex));
    }

    try {
      bean.intField1 = 3;
      dag.validate();
      Assert.fail("should throw ConstraintViolationException");
    } catch (ConstraintViolationException e) {
      ConstraintViolation<?> cv2 = e.getConstraintViolations().iterator().next();
      Assert.assertEquals("" + e.getConstraintViolations(), 1, constraintViolations.size());
      Assert.assertEquals("", false, cv2.getInvalidValue());
      Assert.assertEquals("", "validConfiguration", cv2.getPropertyPath().toString());
    }
    bean.stringField1 = "malhar3";

    // annotated getter
    try {
      bean.getterProperty2 = null;
      dag.validate();
      Assert.fail("should throw ConstraintViolationException");
    } catch (ConstraintViolationException e) {
      ConstraintViolation<?> cv2 = e.getConstraintViolations().iterator().next();
      Assert.assertEquals("" + e.getConstraintViolations(), 1, constraintViolations.size());
      Assert.assertEquals("", null, cv2.getInvalidValue());
      Assert.assertEquals("", "property2", cv2.getPropertyPath().toString());
    }
    bean.getterProperty2 = "";

    // nested property
    try {
      bean.nestedBean.property = null;
      dag.validate();
      Assert.fail("should throw ConstraintViolationException");
    } catch (ConstraintViolationException e) {
      ConstraintViolation<?> cv2 = e.getConstraintViolations().iterator().next();
      Assert.assertEquals("" + e.getConstraintViolations(), 1, constraintViolations.size());
      Assert.assertEquals("", null, cv2.getInvalidValue());
      Assert.assertEquals("", "nestedBean.property", cv2.getPropertyPath().toString());
    }
    bean.nestedBean.property = "";

    // all valid
    dag.validate();

  }

  @OperatorAnnotation(partitionable = false)
  public static class TestOperatorAnnotationOperator extends BaseOperator {

    @InputPortFieldAnnotation( optional = true)
    final public transient DefaultInputPort<Object> input1 = new DefaultInputPort<Object>() {
      @Override
      public void process(Object tuple) {
      }
    };
  }

  class NoInputPortOperator extends BaseOperator {
  }

  @Test
  public void testValidationForNonInputRootOperator() {
    LogicalPlan dag = new LogicalPlan();
    NoInputPortOperator x = dag.addOperator("x", new NoInputPortOperator());
    try {
      dag.validate();
      Assert.fail("should fail because root operator is not input operator");
    } catch (ValidationException e) {
      // expected
    }
  }

  @OperatorAnnotation(partitionable = false)
  public static class TestOperatorAnnotationOperator2 extends BaseOperator implements Partitioner<TestOperatorAnnotationOperator2> {

    @Override
    public Collection<Partition<TestOperatorAnnotationOperator2>> definePartitions(Collection<Partition<TestOperatorAnnotationOperator2>> partitions, PartitioningContext context)
    {
      return null;
    }

    @Override
    public void partitioned(Map<Integer, Partition<TestOperatorAnnotationOperator2>> partitions)
    {
    }
  }

  @Test
  public void testOperatorAnnotation() {
    LogicalPlan dag = new LogicalPlan();
    TestGeneratorInputOperator input = dag.addOperator("input1", TestGeneratorInputOperator.class);
    TestOperatorAnnotationOperator operator = dag.addOperator("operator1", TestOperatorAnnotationOperator.class);
    dag.addStream("Connection", input.outport, operator.input1);


    dag.setAttribute(operator, OperatorContext.PARTITIONER, new StatelessPartitioner<TestOperatorAnnotationOperator>(2));

    try {
      dag.validate();
      Assert.fail("should raise operator is not partitionable for operator1");
    } catch (ValidationException e) {
      Assert.assertEquals("", "Operator " + dag.getMeta(operator).getName() + " provides partitioning capabilities but the annotation on the operator class declares it non partitionable!", e.getMessage());
    }

    dag.setAttribute(operator, OperatorContext.PARTITIONER, null);
    dag.setInputPortAttribute(operator.input1, PortContext.PARTITION_PARALLEL, true);

    try {
      dag.validate();
      Assert.fail("should raise operator is not partitionable for operator1");
    } catch (ValidationException e) {
      Assert.assertEquals("", "Operator " + dag.getMeta(operator).getName() + " is not partitionable but PARTITION_PARALLEL attribute is set", e.getMessage());
    }

    dag.setInputPortAttribute(operator.input1, PortContext.PARTITION_PARALLEL, false);
    dag.validate();

    dag.removeOperator(operator);
    TestOperatorAnnotationOperator2 operator2 = dag.addOperator("operator2", TestOperatorAnnotationOperator2.class);

    try {
      dag.validate();
      Assert.fail("should raise operator is not partitionable for operator2");
    } catch (ValidationException e) {
      Assert.assertEquals("Operator " + dag.getMeta(operator2).getName() + " provides partitioning capabilities but the annotation on the operator class declares it non partitionable!", e.getMessage());
    }
  }

  @Test
  public void testPortConnectionValidation() {

    LogicalPlan dag = new LogicalPlan();

    TestNonOptionalOutportInputOperator input = dag.addOperator("input1", TestNonOptionalOutportInputOperator.class);

    try {
      dag.validate();
      Assert.fail("should raise port not connected for input1.outputPort1");

    } catch (ValidationException e) {
      Assert.assertEquals("", "Output port connection required: input1.outport1", e.getMessage());
    }

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    dag.addStream("stream1", input.outport1, o1.inport1);
    dag.validate();

    // required input
    dag.addOperator("counter", CounterOperator.class);
    try {
      dag.validate();
    } catch (ValidationException e) {
      Assert.assertEquals("", "Input port connection required: counter.countInputPort", e.getMessage());
    }

  }

  @Test
  public void testAtMostOnceProcessingModeValidation() {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    TestGeneratorInputOperator input2 = dag.addOperator("input2", TestGeneratorInputOperator.class);

    GenericTestOperator amoOper = dag.addOperator("amoOper", GenericTestOperator.class);
    dag.setAttribute(amoOper, OperatorContext.PROCESSING_MODE, Operator.ProcessingMode.AT_MOST_ONCE);

    dag.addStream("input1.outport", input1.outport, amoOper.inport1);
    dag.addStream("input2.outport", input2.outport, amoOper.inport2);

    GenericTestOperator outputOper = dag.addOperator("outputOper", GenericTestOperator.class);
    dag.setAttribute(outputOper, OperatorContext.PROCESSING_MODE, Operator.ProcessingMode.AT_LEAST_ONCE);
    dag.addStream("aloOper.outport1", amoOper.outport1, outputOper.inport1);

    try {
      dag.validate();
      Assert.fail("Exception expected for " + outputOper);
    } catch (ValidationException ve) {
      Assert.assertEquals("", ve.getMessage(), "Processing mode outputOper/AT_LEAST_ONCE not valid for source amoOper/AT_MOST_ONCE");
    }
    dag.setAttribute(outputOper, OperatorContext.PROCESSING_MODE, null);
    dag.validate();

    OperatorMeta outputOperOm = dag.getMeta(outputOper);
    Assert.assertEquals("" + outputOperOm.getAttributes(), Operator.ProcessingMode.AT_MOST_ONCE, outputOperOm.getValue(OperatorContext.PROCESSING_MODE));

  }

    @Test
  public void testExactlyOnceProcessingModeValidation() {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    TestGeneratorInputOperator input2 = dag.addOperator("input2", TestGeneratorInputOperator.class);

    GenericTestOperator amoOper = dag.addOperator("amoOper", GenericTestOperator.class);
    dag.setAttribute(amoOper, OperatorContext.PROCESSING_MODE, Operator.ProcessingMode.EXACTLY_ONCE);

    dag.addStream("input1.outport", input1.outport, amoOper.inport1);
    dag.addStream("input2.outport", input2.outport, amoOper.inport2);

    GenericTestOperator outputOper = dag.addOperator("outputOper", GenericTestOperator.class);
    dag.addStream("aloOper.outport1", amoOper.outport1, outputOper.inport1);

    try {
      dag.validate();
      Assert.fail("Exception expected for " + outputOper);
    } catch (ValidationException ve) {
      Assert.assertEquals("", ve.getMessage(), "Processing mode for outputOper should be AT_MOST_ONCE for source amoOper/EXACTLY_ONCE");
    }

    dag.setAttribute(outputOper, OperatorContext.PROCESSING_MODE, Operator.ProcessingMode.AT_LEAST_ONCE);

    try {
      dag.validate();
      Assert.fail("Exception expected for " + outputOper);
    } catch (ValidationException ve) {
      Assert.assertEquals("", ve.getMessage(), "Processing mode outputOper/AT_LEAST_ONCE not valid for source amoOper/EXACTLY_ONCE");
    }

    // AT_MOST_ONCE is valid
    dag.setAttribute(outputOper, OperatorContext.PROCESSING_MODE, Operator.ProcessingMode.AT_MOST_ONCE);
    dag.validate();
  }

  @Test
  public void testLocalityValidation() {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    StreamMeta s1 = dag.addStream("input1.outport", input1.outport, o1.inport1).setLocality(Locality.THREAD_LOCAL);
    dag.validate();

    TestGeneratorInputOperator input2 = dag.addOperator("input2", TestGeneratorInputOperator.class);
    dag.addStream("input2.outport", input2.outport, o1.inport2);

    try {
      dag.validate();
      Assert.fail("Exception expected for " + o1);
    } catch (ValidationException ve) {
      Assert.assertThat("", ve.getMessage(), RegexMatcher.matches("Locality THREAD_LOCAL invalid for operator .* with multiple input streams .*"));
    }

    s1.setLocality(null);
    dag.validate();
  }

  private class TestAnnotationsOperator extends BaseOperator implements InputOperator {
    //final public transient DefaultOutputPort<Object> outport1 = new DefaultOutputPort<Object>();

    @OutputPortFieldAnnotation( optional=false)
    final public transient DefaultOutputPort<Object> outport2 = new DefaultOutputPort<Object>();

    @Override
    public void emitTuples() {
      // Emit Nothing

    }
  }

  private class TestAnnotationsOperator2 extends BaseOperator implements InputOperator{
    // multiple ports w/o annotation, one of them must be connected
    final public transient DefaultOutputPort<Object> outport1 = new DefaultOutputPort<Object>();

    @Override
    public void emitTuples() {
      // Emit Nothing

    }
  }

  private class TestAnnotationsOperator3 extends BaseOperator implements InputOperator{
    // multiple ports w/o annotation, one of them must be connected
    @OutputPortFieldAnnotation( optional=true)
    final public transient DefaultOutputPort<Object> outport1 = new DefaultOutputPort<Object>();
    @OutputPortFieldAnnotation( optional=true)
    final public transient DefaultOutputPort<Object> outport2 = new DefaultOutputPort<Object>();
    @Override
    public void emitTuples() {
      // Emit Nothing

    }
  }

  @Test
  public void testOutputPortAnnotation() {
    LogicalPlan dag = new LogicalPlan();
    TestAnnotationsOperator ta1 = dag.addOperator("testAnnotationsOperator", new TestAnnotationsOperator());

    try {
      dag.validate();
      Assert.fail("should raise: port connection required");
    } catch (ValidationException e) {
      Assert.assertEquals("", "Output port connection required: testAnnotationsOperator.outport2", e.getMessage());
    }

    TestOutputOperator o2 = dag.addOperator("sink", new TestOutputOperator());
    dag.addStream("s1", ta1.outport2, o2.inport);

    dag.validate();

    TestAnnotationsOperator2 ta2 = dag.addOperator("multiOutputPorts1", new TestAnnotationsOperator2());

    try {
      dag.validate();
      Assert.fail("should raise: At least one output port must be connected");
    } catch (ValidationException e) {
      Assert.assertEquals("", "At least one output port must be connected: multiOutputPorts1", e.getMessage());
    }
    TestOutputOperator o3 = dag.addOperator("o3", new TestOutputOperator());
    dag.addStream("s2", ta2.outport1, o3.inport);

    dag.addOperator("multiOutputPorts3", new TestAnnotationsOperator3());
    dag.validate();

  }

  /**
   * Operator that can be used with default Java serialization instead of Kryo
   */
  @DefaultSerializer(JavaSerializer.class)
  public static class JdkSerializableOperator extends BaseOperator implements Serializable {
    private static final long serialVersionUID = -4024202339520027097L;

    public abstract class SerializableInputPort<T> implements InputPort<T>, Sink<T>, java.io.Serializable {
      private static final long serialVersionUID = 1L;

      @Override
      public Sink<T> getSink() {
        return this;
      }

      @Override
      public void setConnected(boolean connected) {
      }

      @Override
      public void setup(PortContext context)
      {
      }

      @Override
      public void teardown()
      {
      }

      @Override
      public StreamCodec<T> getStreamCodec() {
        return null;
      }
    }

    @InputPortFieldAnnotation( optional=true)
    final public InputPort<Object> inport1 = new SerializableInputPort<Object>() {
      private static final long serialVersionUID = 1L;

      @Override
      final public void put(Object payload)
      {
      }

      @Override
      public int getCount(boolean reset)
      {
        return 0;
      }

    };
  }

  @Test
  public void testJdkSerializableOperator() throws Exception {
    LogicalPlan dag = new LogicalPlan();
    dag.addOperator("o1", new JdkSerializableOperator());

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    LogicalPlan.write(dag, outStream);
    outStream.close();

    LogicalPlan clonedDag = LogicalPlan.read(new ByteArrayInputStream(outStream.toByteArray()));
    JdkSerializableOperator o1Clone = (JdkSerializableOperator)clonedDag.getOperatorMeta("o1").getOperator();
    Assert.assertNotNull("port object null", o1Clone.inport1);
  }

  @Test
  public void testAttributeValuesSerializableCheck() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
  {
    LogicalPlan dag = new LogicalPlan();
    Attribute<Object> attr = new Attribute<Object>(new TestAttributeValue(), new Object2String());
    Field nameField = Attribute.class.getDeclaredField("name");
    nameField.setAccessible(true);
    nameField.set(attr, "Test_Attribute");
    nameField.setAccessible(false);

    assertNotNull(attr);
    // Dag attribute not serializable test
    dag.setAttribute(attr, new TestAttributeValue());
    try {
      dag.validate();
      Assert.fail("Setting not serializable attribute should throw exception");
    } catch (ValidationException e) {
      assertEquals("Validation Exception should match ", "Attribute value(s) for Test_Attribute in com.datatorrent.api.DAG are not serializable", e.getMessage());
    }

    // Operator attribute not serializable test
    dag = new LogicalPlan();
    TestGeneratorInputOperator operator = dag.addOperator("TestOperator", TestGeneratorInputOperator.class);
    dag.setAttribute(operator, attr, new TestAttributeValue());
    try {
      dag.validate();
      Assert.fail("Setting not serializable attribute should throw exception");
    } catch (ValidationException e) {
      assertEquals("Validation Exception should match ", "Attribute value(s) for Test_Attribute in TestOperator are not serializable", e.getMessage());
    }

    // Output Port attribute not serializable test
    dag = new LogicalPlan();
    operator = dag.addOperator("TestOperator", TestGeneratorInputOperator.class);
    dag.setOutputPortAttribute(operator.outport, attr, new TestAttributeValue());
    try {
      dag.validate();
      Assert.fail("Setting not serializable attribute should throw exception");
    } catch (ValidationException e) {
      assertEquals("Validation Exception should match ", "Attribute value(s) for Test_Attribute in TestOperator.outport are not serializable", e.getMessage());
    }

    // Input Port attribute not serializable test
    dag = new LogicalPlan();
    GenericTestOperator operator1 = dag.addOperator("TestOperator", GenericTestOperator.class);
    dag.setInputPortAttribute(operator1.inport1, attr, new TestAttributeValue());
    try {
      dag.validate();
      Assert.fail("Setting non serializable attribute should throw exception");
    } catch (ValidationException e) {
      assertEquals("Validation Exception should match ", "Attribute value(s) for Test_Attribute in TestOperator.inport1 are not serializable", e.getMessage());
    }
  }

  private static class Object2String implements StringCodec<Object>
  {

    @Override
    public Object fromString(String string)
    {
      // Stub method for testing - do nothing
      return null;
    }

    @Override
    public String toString(Object pojo)
    {
      // Stub method for testing - do nothing
      return null;
    }

  }

  private static class TestAttributeValue
  {
  }

  private static class TestStreamCodec implements StreamCodec<Object>
  {
    @Override
    public Object fromByteArray(Slice fragment)
    {
      return fragment.stringValue();
    }

    @Override
    public Slice toByteArray(Object o)
    {
      byte[] b = o.toString().getBytes();
      return new Slice(b, 0, b.length);
    }

    @Override
    public int getPartition(Object o)
    {
      return o.hashCode() / 2;
    }
  }

  public static class TestPortCodecOperator extends BaseOperator {
    public transient final DefaultInputPort<Object> inport1 = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {

      }

      @Override
      public StreamCodec<Object> getStreamCodec()
      {
        return new TestStreamCodec();
      }
    };

    @OutputPortFieldAnnotation( optional = true)
    public transient final DefaultOutputPort<Object> outport = new DefaultOutputPort<Object>();
  }

  /*
  @Test
  public void testStreamCodec() throws Exception {
    LogicalPlan dag = new LogicalPlan();
    TestGeneratorInputOperator input = dag.addOperator("input", TestGeneratorInputOperator.class);
    GenericTestOperator gto1 = dag.addOperator("gto1", GenericTestOperator.class);
    StreamMeta stream1 = dag.addStream("s1", input.outport, gto1.inport1);
    StreamCodec<?> codec1 = new TestStreamCodec();
    dag.setInputPortAttribute(gto1.inport1, PortContext.STREAM_CODEC, codec1);
    dag.validate();
    //Assert.assertEquals("Stream codec not set", stream1.getStreamCodec(), codec1);

    GenericTestOperator gto2 = dag.addOperator("gto2", GenericTestOperator.class);
    GenericTestOperator gto3 = dag.addOperator("gto3", GenericTestOperator.class);
    StreamMeta stream2 = dag.addStream("s2", gto1.outport1, gto2.inport1, gto3.inport1);
    dag.setInputPortAttribute(gto2.inport1, PortContext.STREAM_CODEC, codec1);
    try {
      dag.validate();
    } catch (ValidationException e) {
      String msg = e.getMessage();
      if (!msg.startsWith("Stream codec not set on input port") || !msg.contains("gto3")
              || !msg.contains(codec1.toString()) || !msg.endsWith("was specified on another port")) {
        Assert.fail(String.format("LogicalPlan validation error msg: %s", msg));
      }
    }

    dag.setInputPortAttribute(gto3.inport1, PortContext.STREAM_CODEC, codec1);
    dag.validate();
    //Assert.assertEquals("Stream codec not set", stream2.getStreamCodec(), codec1);

    StreamCodec<?> codec2 = new TestStreamCodec();
    dag.setInputPortAttribute(gto3.inport1, PortContext.STREAM_CODEC, codec2);
    try {
      dag.validate();
    } catch (ValidationException e) {
      String msg = e.getMessage();
      if (!msg.startsWith("Conflicting stream codec set on input port") || !msg.contains("gto3")
              || !msg.contains(codec2.toString()) || !msg.endsWith("was specified on another port")) {
        Assert.fail(String.format("LogicalPlan validation error msg: %s", msg));
      }
    }

    dag.setInputPortAttribute(gto3.inport1, PortContext.STREAM_CODEC, codec1);
    TestPortCodecOperator pco = dag.addOperator("pco", TestPortCodecOperator.class);
    StreamMeta stream3 = dag.addStream("s3", gto2.outport1, pco.inport1);
    dag.validate();
    //Assert.assertEquals("Stream codec class not set", stream3.getCodecClass(), TestStreamCodec.class);

    dag.setInputPortAttribute(pco.inport1, PortContext.STREAM_CODEC, codec2);
    dag.validate();
    //Assert.assertEquals("Stream codec not set", stream3.getStreamCodec(), codec2);
  }
  */

  @Test
  public void testCheckpointableWithinAppWindowAnnotation()
  {
    LogicalPlan dag = new LogicalPlan();
    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    GenericTestOperator x = dag.addOperator("x", new GenericTestOperator());
    dag.addStream("Stream1", input1.outport, x.inport1);
    dag.setAttribute(x, OperatorContext.CHECKPOINT_WINDOW_COUNT, 15);
    dag.setAttribute(x, OperatorContext.APPLICATION_WINDOW_COUNT, 30);
    dag.validate();

    TestGeneratorInputOperator input2 = dag.addOperator("input2", TestGeneratorInputOperator.class);
    CheckpointableWithinAppWindowOperator y = dag.addOperator("y", new CheckpointableWithinAppWindowOperator());
    dag.addStream("Stream2", input2.outport, y.inport1);
    dag.setAttribute(y, OperatorContext.CHECKPOINT_WINDOW_COUNT, 15);
    dag.setAttribute(y, OperatorContext.APPLICATION_WINDOW_COUNT, 30);
    dag.validate();

    TestGeneratorInputOperator input3 = dag.addOperator("input3", TestGeneratorInputOperator.class);
    NotCheckpointableWithinAppWindowOperator z = dag.addOperator("z", new NotCheckpointableWithinAppWindowOperator());
    dag.addStream("Stream3", input3.outport, z.inport1);
    dag.setAttribute(z, OperatorContext.CHECKPOINT_WINDOW_COUNT, 15);
    dag.setAttribute(z, OperatorContext.APPLICATION_WINDOW_COUNT, 30);
    try {
      dag.validate();
      Assert.fail("should fail because chekpoint window count is not a factor of application window count");
    }
    catch (ValidationException e) {
      // expected
    }

    dag.setAttribute(z, OperatorContext.CHECKPOINT_WINDOW_COUNT, 30);
    dag.validate();

    dag.setAttribute(z, OperatorContext.CHECKPOINT_WINDOW_COUNT, 45);
    try {
      dag.validate();
      Assert.fail("should fail because chekpoint window count is not a factor of application window count");
    }
    catch (ValidationException e) {
      // expected
    }
  }

  @OperatorAnnotation(checkpointableWithinAppWindow = true)
  class CheckpointableWithinAppWindowOperator extends GenericTestOperator
  {
  }

  @OperatorAnnotation(checkpointableWithinAppWindow = false)
  class NotCheckpointableWithinAppWindowOperator extends GenericTestOperator
  {
  }

  @Test
  public void testInputPortHiding()
  {
    LogicalPlan dag = new LogicalPlan();
    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    Operator2 operator2 = dag.addOperator("operator2", new Operator2());
    dag.addStream("Stream1", input1.outport, operator2.input);
    dag.validate();
  }

  @Test
  public void testInvalidInputPortConnection()
  {
    LogicalPlan dag = new LogicalPlan();
    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    Operator1 operator1 = dag.addOperator("operator3", new Operator3());
    dag.addStream("Stream1", input1.outport, operator1.input);
    try {
      dag.validate();
    } catch (ValidationException ex) {
      Assert.assertTrue("validation message", ex.getMessage().startsWith("Invalid port connected"));
      return;
    }
    Assert.fail();
  }

  class Operator1 extends BaseOperator
  {
    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {

      }
    };
  }

  class Operator2 extends Operator1
  {
    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {

      }
    };
  }

  class Operator3 extends Operator1
  {
    @InputPortFieldAnnotation(optional = true)
    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {

      }
    };
  }

  @Test
  public void testOutputPortHiding()
  {
    LogicalPlan dag = new LogicalPlan();
    Operator5 operator5 = dag.addOperator("input", new Operator5());
    Operator2 operator2 = dag.addOperator("operator2", new Operator2());
    dag.addStream("Stream1", operator5.output, operator2.input);
    dag.validate();
  }

  @Test(expected = ValidationException.class)
  public void testInvalidOutputPortConnection()
  {
    LogicalPlan dag = new LogicalPlan();
    Operator4 operator4 = dag.addOperator("input", new Operator5());
    Operator3 operator3 = dag.addOperator("operator3", new Operator3());
    dag.addStream("Stream1", operator4.output, operator3.input);
    dag.validate();
  }

  class Operator4 extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

    @Override
    public void emitTuples()
    {

    }
  }

  class Operator5 extends Operator4
  {
    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();
  }

  /*
  These were tests for operator semantics that verified if an operator class implements InputOperator then the same class should not declare input ports.
  This would be done later when we are able to verify user code at compile-time.

    validation()
  {
    if (n.getOperator() instanceof InputOperator) {
      try {
        for (Class<?> clazz : n.getOperator().getClass().getInterfaces()) {
          if (clazz.getName().equals(InputOperator.class.getName())) {
            for (Field field : n.getOperator().getClass().getDeclaredFields()) {
              field.setAccessible(true);
              Object declaredObject = field.get(n.getOperator());
              if (declaredObject instanceof InputPort) {
                throw new ValidationException("Operator class implements InputOperator and also declares input ports: " + n.name);
              }
            }
            break;
          }
        }
      }
      catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }
  @Test
  public void testInvalidInputOperatorDeclaration()
  {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator.InvalidInputOperator inputOperator = dag.addOperator("input", new TestGeneratorInputOperator.InvalidInputOperator());
    GenericTestOperator operator2 = dag.addOperator("operator2", GenericTestOperator.class);

    dag.addStream("stream1", inputOperator.outport, operator2.inport1);

    try {
      dag.validate();
      fail("validation should fail");
    }
    catch (ValidationException e) {
      // expected
    }
  }

  @Test
  public void testValidInputOperatorDeclaration()
  {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator.ValidGenericOperator operator1 = dag.addOperator("input", new TestGeneratorInputOperator.ValidGenericOperator());
    GenericTestOperator operator2 = dag.addOperator("operator2", GenericTestOperator.class);

    dag.addStream("stream1", operator1.outport, operator2.inport1);
    dag.validate();
  }
  */
}
