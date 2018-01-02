package com.datatorrent.stram.cli.util;

public class Arg
{
  final String name;

  public Arg(String name)
  {
    this.name = name;
  }

  @Override
  public String toString()
  {
    return name;
  }

}
