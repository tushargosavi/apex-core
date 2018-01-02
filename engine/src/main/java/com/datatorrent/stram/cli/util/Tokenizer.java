package com.datatorrent.stram.cli.util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.datatorrent.stram.cli.ApexCli;

public class Tokenizer
{
  ApexCli apexCli;

  public Tokenizer(ApexCli apexCli)
  {
    this.apexCli = apexCli;
  }

  private void appendToCommandBuffer(List<String> commandBuffer, StringBuffer buf, boolean potentialEmptyArg)
  {
    if (potentialEmptyArg || buf.length() > 0) {
      commandBuffer.add(buf.toString());
      buf.setLength(0);
    }
  }

  private List<String> startNewCommand(LinkedList<List<String>> resultBuffer)
  {
    List<String> newCommand = new ArrayList<>();
    if (!resultBuffer.isEmpty()) {
      List<String> lastCommand = resultBuffer.peekLast();
      if (lastCommand.size() == 1) {
        String first = lastCommand.get(0);
        if (first.matches("^[A-Za-z][A-Za-z0-9]*=.*")) {
          // This is a variable assignment
          int equalSign = first.indexOf('=');
          apexCli.getVariableMap().put(first.substring(0, equalSign), first.substring(equalSign + 1));
          resultBuffer.removeLast();
        }
      }
    }
    resultBuffer.add(newCommand);
    return newCommand;
  }

  public List<String[]> tokenize(String commandLine)
  {
    LinkedList<List<String>> resultBuffer = new LinkedList<>();
    List<String> commandBuffer = startNewCommand(resultBuffer);

    if (commandLine != null) {
      commandLine = ApexCli.ltrim(commandLine);
      if (commandLine.startsWith("#")) {
        return null;
      }

      int len = commandLine.length();
      boolean insideQuotes = false;
      boolean potentialEmptyArg = false;
      StringBuffer buf = new StringBuffer(commandLine.length());

      for (@SuppressWarnings("AssignmentToForLoopParameter") int i = 0; i < len; ++i) {
        char c = commandLine.charAt(i);
        if (c == '"') {
          potentialEmptyArg = true;
          insideQuotes = !insideQuotes;
        } else if (c == '\\') {
          if (len > i + 1) {
            switch (commandLine.charAt(i + 1)) {
              case 'n':
                buf.append("\n");
                break;
              case 't':
                buf.append("\t");
                break;
              case 'r':
                buf.append("\r");
                break;
              case 'b':
                buf.append("\b");
                break;
              case 'f':
                buf.append("\f");
                break;
              default:
                buf.append(commandLine.charAt(i + 1));
            }
            ++i;
          }
        } else {
          if (insideQuotes) {
            buf.append(c);
          } else {

            if (c == '$') {
              StringBuilder variableName = new StringBuilder(32);
              if (len > i + 1) {
                if (commandLine.charAt(i + 1) == '{') {
                  ++i;
                  while (len > i + 1) {
                    char ch = commandLine.charAt(i + 1);
                    if (ch != '}') {
                      variableName.append(ch);
                    }
                    ++i;
                    if (ch == '}') {
                      break;
                    }
                    if (len <= i + 1) {
                      throw new CliException("Parse error: unmatched brace");
                    }
                  }
                } else if (commandLine.charAt(i + 1) == '?') {
                  ++i;
                  buf.append(ApexCli.isLastCommandError() ? "1" : "0");
                  continue;
                } else {
                  while (len > i + 1) {
                    char ch = commandLine.charAt(i + 1);
                    if ((variableName.length() > 0 && ch >= '0' && ch <= '9') || ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'))) {
                      variableName.append(ch);
                    } else {
                      break;
                    }
                    ++i;
                  }
                }
                if (variableName.length() == 0) {
                  buf.append(c);
                } else {
                  String value = apexCli.getVariableMap().get(variableName.toString());
                  if (value != null) {
                    buf.append(value);
                  }
                }
              } else {
                buf.append(c);
              }
            } else if (c == ';') {
              appendToCommandBuffer(commandBuffer, buf, potentialEmptyArg);
              commandBuffer = startNewCommand(resultBuffer);
            } else if (Character.isWhitespace(c)) {
              appendToCommandBuffer(commandBuffer, buf, potentialEmptyArg);
              potentialEmptyArg = false;
              if (len > i + 1 && commandLine.charAt(i + 1) == '#') {
                break;
              }
            } else {
              buf.append(c);
            }
          }
        }
      }
      appendToCommandBuffer(commandBuffer, buf, potentialEmptyArg);
    }
    startNewCommand(resultBuffer);
    List<String[]> result = new ArrayList<>();
    for (List<String> command : resultBuffer) {
      String[] commandArray = new String[command.size()];
      result.add(command.toArray(commandArray));
    }
    return result;
  }
}
