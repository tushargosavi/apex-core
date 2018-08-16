/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.local;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * TODO: Replace trivial implementations of methods as needed
 */
public class StramClient extends com.datatorrent.stram.StramClient
{
  public StramClient(Configuration conf, LogicalPlan dag) throws Exception
  {
    super(conf, dag);
  }

  @Override
  public void start()
  {

  }

  @Override
  public void stop()
  {

  }

  @Override
  public void startApplication() throws Exception
  {

  }

  @Override
  public void killApplication() throws Exception
  {

  }

  @Override
  public boolean monitorApplication() throws Exception
  {
    return false;
  }
}
