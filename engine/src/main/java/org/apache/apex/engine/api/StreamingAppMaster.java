/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.api;

import com.datatorrent.api.Component;

public interface StreamingAppMaster extends Component<StreamingAppMasterContext>
{
  //TODO: These are properties for a specific loader, find a better place
  String PLUGINS_CONF_KEY = "apex.plugin.stram.plugins";
  String PLUGINS_CONF_SEP = ",";

  boolean run() throws Exception;
}
