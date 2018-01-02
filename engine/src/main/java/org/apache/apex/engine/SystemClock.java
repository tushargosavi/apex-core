/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine;

import org.apache.apex.engine.api.Clock;

public class SystemClock implements Clock
{
  @Override
  public long getTime()
  {
    return System.currentTimeMillis();
  }
}
