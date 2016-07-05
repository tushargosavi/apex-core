package com.datatorrent.stram.plan.physical;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.datatorrent.api.Context;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.InputOperatorTest;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.TestPlanContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.mod.DAGChangeSetImpl;
import com.datatorrent.stram.support.StramTestSupport;

public class PhysicalPlanExtendTests
{
  /**
   * Test to extend physical DAG at runtime.
   */
  @Rule
  public StramTestSupport.TestMeta testMeta = new StramTestSupport.TestMeta();

  @Test
  public void extendPhysicalPlan()
  {
    LogicalPlan dag = StramTestSupport.createDAG(testMeta);
    GenericTestOperator o1 = dag.addOperator("o1", new GenericTestOperator());
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new TestPlanContext());

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    Assert.assertEquals("number of operators in physical plan", 1, plan.getAllOperators().size());
    //plan.deployChanges();
    Assert.assertEquals("new operators to deploy stage 1 ", 1, ctx.deploy.size());

    DAGChangeSetImpl newDag = new DAGChangeSetImpl();
    GenericTestOperator o2 = newDag.addOperator("o2", new GenericTestOperator());
    GenericTestOperator o3 = newDag.addOperator("o3", new GenericTestOperator());
    GenericTestOperator o4 = newDag.addOperator("o4", new GenericTestOperator());
    GenericTestOperator o5 = newDag.addOperator("o5", new GenericTestOperator());

    newDag.addStream("s1", o2.outport1, o3.inport1);
    newDag.addStream("s2", o3.outport1, o4.inport1);
    newDag.addStream("s3", o4.outport1, o5.inport1);

    PlanModifier pm = new PlanModifier(plan);
    pm.applyDagChangeSet(newDag);

    Assert.assertEquals("new operators in the Dag is ", 5, plan.getAllOperators().size());
    Assert.assertEquals("number of deploy operators ", 4, ctx.deploy.size());
  }

  @Test
  public void extendPhysicalPlanSameDag()
  {
    LogicalPlan dag = StramTestSupport.createDAG(testMeta);
    InputOperatorTest.EvenOddIntegerGeneratorInputOperator o1 = dag.addOperator("o1", new InputOperatorTest.EvenOddIntegerGeneratorInputOperator());
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new TestPlanContext());
    dag.validate();

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    Assert.assertEquals("number of operators in physical plan", 1, plan.getAllOperators().size());
    Assert.assertEquals("new operators to deploy stage 1 ", 1, ctx.deploy.size());

    DAGChangeSetImpl newDag = new DAGChangeSetImpl();
    GenericTestOperator o2 = newDag.addOperator("o2", new GenericTestOperator());
    GenericTestOperator o3 = newDag.addOperator("o3", new GenericTestOperator());

    newDag.addStream("s1", "o1", "even", o2.inport1);
    newDag.addStream("s2", o2.outport1, o3.inport1);

    PlanModifier pm = new PlanModifier(plan);
    pm.applyDagChangeSet(newDag);
    plan.getLogicalPlan().validate();

    Assert.assertEquals("new operators in the Dag is ", 3, plan.getAllOperators().size());
    Assert.assertEquals("number of deploy operators ", 3, ctx.deploy.size());

    DAGChangeSetImpl removeSet = new DAGChangeSetImpl();
    for (String s : new String[]{"s2", "s1"}) {
      removeSet.removeStream(s);
    }
    for (String opr : new String[]{"o2", "o3"}) {
      removeSet.removeOperator(opr);
    }
    pm = new PlanModifier(plan);
    pm.applyDagChangeSet(removeSet);
    plan.getLogicalPlan().validate();
    Assert.assertEquals("Number of operators in Dag ", 1, plan.getLogicalPlan().getAllOperators().size());
  }

  @Test
  public void understandTest()
  {
    LogicalPlan dag = new LogicalPlan();
    TestGeneratorInputOperator o1 = dag.addOperator("o1", new TestGeneratorInputOperator());
    GenericTestOperator o2 = dag.addOperator("o2", new GenericTestOperator());
    dag.addStream("s1", o1.outport, o2.inport1);

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);

    System.out.println("hey there");
  }
}
