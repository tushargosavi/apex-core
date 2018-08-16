package com.datatorrent.stram.cli.util;

public class CliException extends RuntimeException
{
  private static final long serialVersionUID = 1L;

  public CliException(String msg, Throwable cause)
  {
    super(msg, cause);
  }

  public CliException(String msg)
  {
    super(msg);
  }
}
