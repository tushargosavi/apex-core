package com.datatorrent.stram.cli.util;

import java.util.List;

import jline.console.completer.Completer;

public final class MyNullCompleter implements Completer
{
  public static final MyNullCompleter INSTANCE = new MyNullCompleter();

  @Override
  public int complete(final String buffer, final int cursor, final List<CharSequence> candidates)
  {
    candidates.add("");
    return cursor;
  }
}

