/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.api;

import org.apache.commons.cli.CommandLine;

import com.datatorrent.api.Context;

public interface StreamingAppMasterContext extends Context
{
  CommandLine getCommandLine();
}
