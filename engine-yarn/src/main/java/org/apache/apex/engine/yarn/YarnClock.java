/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.yarn;

import org.apache.hadoop.yarn.util.Clock;

public class YarnClock implements org.apache.apex.engine.api.Clock
{
  Clock clock;

  public YarnClock(Clock clock)
  {
    this.clock = clock;
  }

  @Override
  public long getTime()
  {
    return clock.getTime();
  }
}
