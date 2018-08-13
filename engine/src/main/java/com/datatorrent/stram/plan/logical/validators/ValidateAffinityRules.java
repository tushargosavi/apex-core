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
package com.datatorrent.stram.plan.logical.validators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.validation.ValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.AffinityRule;
import com.datatorrent.api.AffinityRulesSet;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.OperatorPair;

public class ValidateAffinityRules implements ComponentValidator<LogicalPlan>
{
  private static final Logger LOG = LoggerFactory.getLogger(ValidateAffinityRules.class);
  private LogicalPlan dag;
  private List<ValidationIssue> issues = new ArrayList<>();

  /**
   * Validates that operators in Affinity Rule are valid: Checks that operator names are part of the dag and adds them to map of rules
   * @param affinitiesMap
   * @param rule
   * @param operators
   */
  private void addToMap(HashMap<OperatorPair, AffinityRule> affinitiesMap, AffinityRule rule, OperatorPair operators)
  {
    LogicalPlan.OperatorMeta operator1 = dag.getOperatorMeta(operators.first);
    LogicalPlan.OperatorMeta operator2 = dag.getOperatorMeta(operators.second);
    if (operator1 == null || operator2 == null) {
      if (operator1 == null && operator2 == null) {
        throw new ValidationException(String.format("Operators %s & %s specified in affinity rule are not part of the dag", operators.first, operators.second));
      }
      throw new ValidationException(String.format("Operator %s specified in affinity rule is not part of the dag", operator1 == null ? operators.first : operators.second));
    }
    affinitiesMap.put(operators, rule);
  }

  /**
   * Convert regex in Affinity Rule to list of operators
   * Regex should match at least 2 operators, otherwise rule is not applied
   * @param operatorNames
   * @param rule
   */
  public void convertRegexToList(List<String> operatorNames, AffinityRule rule)
  {
    List<String> operators = new LinkedList<>();
    Pattern p = Pattern.compile(rule.getOperatorRegex());
    for (String name : operatorNames) {
      if (p.matcher(name).matches()) {
        operators.add(name);
      }
    }
    rule.setOperatorRegex(null);
    if (operators.size() <= 1) {
      LOG.warn("Regex should match at least 2 operators to add affinity rule. Ignoring rule");
    } else {
      rule.setOperatorsList(operators);
    }
  }

  /**
   * Combine affinity sets for operators with affinity
   * @param containerAffinities
   * @param pair
   */
  public void combineSets(HashMap<String, Set<String>> containerAffinities, OperatorPair pair)
  {
    Set<String> set1 = containerAffinities.get(pair.first);
    Set<String> set2 = containerAffinities.get(pair.second);
    set1.addAll(set2);
    containerAffinities.put(pair.first, set1);
    containerAffinities.put(pair.second, set1);
  }

  /**
   * Get host mapping for an operator using affinity settings and host locality specified for operator
   * @param nodeAffinities
   * @param operator
   * @param hostNamesMapping
   * @return
   */
  public String getHostLocality(HashMap<String, Set<String>> nodeAffinities, String operator, HashMap<String, String> hostNamesMapping)
  {
    if (hostNamesMapping.containsKey(operator)) {
      return hostNamesMapping.get(operator);
    }

    for (String op : nodeAffinities.get(operator)) {
      if (hostNamesMapping.containsKey(op)) {
        return hostNamesMapping.get(op);
      }
    }

    return null;
  }

  /**
   * validation for affinity rules validates following:
   *  1. The operator names specified in affinity rule are part of the dag
   *  2. Affinity rules do not conflict with anti-affinity rules directly or indirectly
   *  3. Anti-affinity rules do not conflict with Stream Locality
   *  4. Anti-affinity rules do not conflict with host-locality attribute
   *  5. Affinity rule between non stream operators does not have Thread_Local locality
   *  6. Affinity rules do not conflict with host-locality attribute
   */
  private void validateAffinityRules()
  {
    AffinityRulesSet affinityRuleSet = dag.getAttributes().get(Context.DAGContext.AFFINITY_RULES_SET);
    if (affinityRuleSet == null || affinityRuleSet.getAffinityRules() == null) {
      return;
    }

    Collection<AffinityRule> affinityRules = affinityRuleSet.getAffinityRules();

    HashMap<String, Set<String>> containerAffinities = new HashMap<>();
    HashMap<String, Set<String>> nodeAffinities = new HashMap<>();
    HashMap<String, String> hostNamesMapping = new HashMap<>();

    HashMap<OperatorPair, AffinityRule> affinities = new HashMap<>();
    HashMap<OperatorPair, AffinityRule> antiAffinities = new HashMap<>();
    HashMap<OperatorPair, AffinityRule> threadLocalAffinities = new HashMap<>();

    List<String> operatorNames = new ArrayList<>();

    for (LogicalPlan.OperatorMeta operator : dag.getAllOperators()) {
      operatorNames.add(operator.getName());
      Set<String> containerSet = new HashSet<>();
      containerSet.add(operator.getName());
      containerAffinities.put(operator.getName(), containerSet);
      Set<String> nodeSet = new HashSet<>();
      nodeSet.add(operator.getName());
      nodeAffinities.put(operator.getName(), nodeSet);

      if (operator.getAttributes().get(OperatorContext.LOCALITY_HOST) != null) {
        hostNamesMapping.put(operator.getName(), operator.getAttributes().get(OperatorContext.LOCALITY_HOST));
      }
    }

    // Identify operators set as Regex and add to list
    for (AffinityRule rule : affinityRules) {
      if (rule.getOperatorRegex() != null) {
        convertRegexToList(operatorNames, rule);
      }
    }
    // Convert operators with list of operator to rules with operator pairs for validation
    for (AffinityRule rule : affinityRules) {
      if (rule.getOperatorsList() != null) {
        List<String> list = rule.getOperatorsList();
        for (int i = 0; i < list.size(); i++) {
          for (int j = i + 1; j < list.size(); j++) {
            OperatorPair pair = new OperatorPair(list.get(i), list.get(j));
            if (rule.getType() == com.datatorrent.api.AffinityRule.Type.AFFINITY) {
              addToMap(affinities, rule, pair);
            } else {
              addToMap(antiAffinities, rule, pair);
            }
          }
        }
      }
    }

    for (Map.Entry<OperatorPair, AffinityRule> ruleEntry : affinities.entrySet()) {
      OperatorPair pair = ruleEntry.getKey();
      AffinityRule rule = ruleEntry.getValue();
      if (hostNamesMapping.containsKey(pair.first) && hostNamesMapping.containsKey(pair.second) && !hostNamesMapping.get(pair.first).equals(hostNamesMapping.get(pair.second))) {
        throw new ValidationException(String.format("Host Locality for operators: %s(host: %s) & %s(host: %s) conflicts with affinity rules", pair.first, hostNamesMapping.get(pair.first), pair.second, hostNamesMapping.get(pair.second)));
      }
      if (rule.getLocality() == DAG.Locality.THREAD_LOCAL) {
        addToMap(threadLocalAffinities, rule, pair);
      } else if (rule.getLocality() == DAG.Locality.CONTAINER_LOCAL) {
        // Combine the sets
        combineSets(containerAffinities, pair);
        // Also update node list
        combineSets(nodeAffinities, pair);
      } else if (rule.getLocality() == DAG.Locality.NODE_LOCAL) {
        combineSets(nodeAffinities, pair);
      }
    }


    for (LogicalPlan.StreamMeta stream : dag.getAllStreams()) {
      String source = stream.getSource().getOperatorMeta().getName();
      for (LogicalPlan.InputPortMeta sink : stream.getSinks()) {
        String sinkOperator = sink.getOperatorMeta().getName();
        OperatorPair pair = new OperatorPair(source, sinkOperator);
        if (stream.getLocality() != null && stream.getLocality().ordinal() <= DAG.Locality.NODE_LOCAL.ordinal() && hostNamesMapping.containsKey(pair.first) && hostNamesMapping.containsKey(pair.second) && !hostNamesMapping.get(pair.first).equals(hostNamesMapping.get(pair.second))) {
          throw new ValidationException(String.format("Host Locality for operators: %s(host: %s) & %s(host: %s) conflicts with stream locality", pair.first, hostNamesMapping.get(pair.first), pair.second, hostNamesMapping.get(pair.second)));
        }
        if (stream.getLocality() == DAG.Locality.CONTAINER_LOCAL) {
          combineSets(containerAffinities, pair);
          combineSets(nodeAffinities, pair);
        } else if (stream.getLocality() == DAG.Locality.NODE_LOCAL) {
          combineSets(nodeAffinities, pair);
        }
        if (affinities.containsKey(pair)) {
          // Choose the lower bound on locality
          AffinityRule rule = affinities.get(pair);
          if (rule.getLocality() == DAG.Locality.THREAD_LOCAL) {
            stream.setLocality(rule.getLocality());
            threadLocalAffinities.remove(rule);
            affinityRules.remove(rule);
          }
          if (stream.getLocality() != null && rule.getLocality().ordinal() > stream.getLocality().ordinal()) {
            // Remove the affinity rule from attributes, as it is redundant
            affinityRules.remove(rule);
          }
        }
      }
    }

    // Validate that all Thread local affinities were for stream connected operators
    if (!threadLocalAffinities.isEmpty()) {
      OperatorPair pair = threadLocalAffinities.keySet().iterator().next();
      throw new ValidationException(String.format("Affinity rule specified THREAD_LOCAL affinity for operators %s & %s which are not connected by stream", pair.first, pair.second));
    }

    for (Map.Entry<OperatorPair, AffinityRule> ruleEntry : antiAffinities.entrySet()) {
      OperatorPair pair = ruleEntry.getKey();
      AffinityRule rule = ruleEntry.getValue();

      if (pair.first.equals(pair.second)) {
        continue;
      }
      if (rule.getLocality() == DAG.Locality.CONTAINER_LOCAL) {
        if (containerAffinities.get(pair.first).contains(pair.second)) {
          throw new ValidationException(String.format("Anti Affinity rule for operators %s & %s conflicts with affinity rules or Stream locality", pair.first, pair.second));

        }
      } else if (rule.getLocality() == DAG.Locality.NODE_LOCAL) {
        if (nodeAffinities.get(pair.first).contains(pair.second)) {
          throw new ValidationException(String.format("Anti Affinity rule for operators %s & %s conflicts with affinity rules or Stream locality", pair.first, pair.second));
        }
        // Check host locality for both operators
        // Check host attribute for all operators in node local set for both
        // anti-affinity operators
        String firstOperatorLocality = getHostLocality(nodeAffinities, pair.first, hostNamesMapping);
        String secondOperatorLocality = getHostLocality(nodeAffinities, pair.second, hostNamesMapping);
        if (firstOperatorLocality != null && secondOperatorLocality != null && firstOperatorLocality == secondOperatorLocality) {
          throw new ValidationException(String.format("Host Locality for operators: %s(host: %s) & %s(host: %s) conflict with anti-affinity rules", pair.first, firstOperatorLocality, pair.second, secondOperatorLocality));
        }
      }
    }
  }

  @Override
  public Collection<? extends ValidationIssue> validate(LogicalPlan dag)
  {
    this.dag = dag;
    validateAffinityRules();
    return issues;
  }
}
