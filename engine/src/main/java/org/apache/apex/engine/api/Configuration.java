/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.api;

public interface Configuration
{

  // Marker interface
  interface Setting<V>
  {
  }

  <V> V get(Setting<V> setting);

  String getVar(Setting<?> setting);

  // TODO:- This should go away in the future
  org.apache.hadoop.conf.Configuration getNativeConfig();

}
