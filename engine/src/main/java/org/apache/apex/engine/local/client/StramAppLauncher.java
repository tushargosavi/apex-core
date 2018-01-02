/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.local.client;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class StramAppLauncher extends com.datatorrent.stram.client.StramAppLauncher
{
  public StramAppLauncher(File appJarFile, Configuration conf) throws Exception
  {
    super(appJarFile, conf);
  }

  public StramAppLauncher(FileSystem fs, Path path, Configuration conf) throws Exception
  {
    super(fs, path, conf);
  }

  public StramAppLauncher(String name, Configuration conf) throws Exception
  {
    super(name, conf);
  }

  public StramAppLauncher(FileSystem fs, Configuration conf) throws Exception
  {
    super(fs, conf);
  }
}
