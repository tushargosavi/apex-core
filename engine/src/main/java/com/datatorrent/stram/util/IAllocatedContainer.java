/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package com.datatorrent.stram.util;

public interface IAllocatedContainer
{
  void stop();

  boolean checkStopRequestedTimeout();
}
