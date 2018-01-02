package com.datatorrent.stram.cli.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.datatorrent.stram.cli.ApexCli;

import jline.console.ConsoleReader;

public class FileLineReader extends ConsoleReader
{
  private final BufferedReader br;

  public FileLineReader(String fileName) throws IOException
  {
    super();
    fileName = ApexCli.expandFileName(fileName, true);
    br = new BufferedReader(new FileReader(fileName));
  }

  @Override
  public String readLine(String prompt) throws IOException
  {
    return br.readLine();
  }

  @Override
  public String readLine(String prompt, Character mask) throws IOException
  {
    return br.readLine();
  }

  @Override
  public String readLine(Character mask) throws IOException
  {
    return br.readLine();
  }

  public void close() throws IOException
  {
    br.close();
  }
}
