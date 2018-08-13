/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.plan.logical;

import com.datatorrent.common.util.Pair;

/**
   * Pair of operator names to specify affinity rule
   * The order of operators is not considered in this class
   * i.e. OperatorPair("O1", "O2") is equal to OperatorPair("O2", "O1")
   */
public class OperatorPair extends Pair<String, String>
{
  private static final long serialVersionUID = 4636942499106381268L;

  public OperatorPair(String first, String second)
  {
    super(first, second);
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj instanceof OperatorPair) {
      OperatorPair pairObj = (OperatorPair)obj;
      return ((this.first.equals(pairObj.first)) && (this.second.equals(pairObj.second)))
          || (this.first.equals(pairObj.second) && this.second.equals(pairObj.first));
    }
    return super.equals(obj);
  }
}
