package com.datatorrent.stram.cli.util;

import java.util.List;

import jline.console.completer.FileNameCompleter;

public final class MyFileNameCompleter extends FileNameCompleter
{
  @Override
  public int complete(final String buffer, final int cursor, final List<CharSequence> candidates)
  {
    int result = super.complete(buffer, cursor, candidates);
    if (candidates.isEmpty()) {
      candidates.add("");
      result = cursor;
    }
    return result;
  }

}

