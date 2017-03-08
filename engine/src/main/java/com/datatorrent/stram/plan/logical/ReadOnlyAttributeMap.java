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

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import com.datatorrent.api.Attribute;

public class ReadOnlyAttributeMap implements Attribute.AttributeMap, Serializable
{
  private final Attribute.AttributeMap parent;

  public ReadOnlyAttributeMap(Attribute.AttributeMap parent)
  {
    this.parent = parent;
  }

  @Override
  public <T> T get(Attribute<T> key)
  {
    return parent.get(key);
  }

  @Override
  public boolean contains(Attribute<?> key)
  {
    return parent.contains(key);
  }

  @Override
  public <T> T put(Attribute<T> key, T value)
  {
    throw new RuntimeException("Can't modify readonly state");
  }

  @Override
  public Set<Map.Entry<Attribute<?>, Object>> entrySet()
  {
    return parent.entrySet();
  }

  @Override
  public Attribute.AttributeMap clone() throws CloneNotSupportedException
  {
    return new ReadOnlyAttributeMap(parent.clone());
  }

  @Override
  public void addAll(Attribute.AttributeMap attrMap)
  {
    throw new RuntimeException("Can not modify read-only attribute map");
  }
}
