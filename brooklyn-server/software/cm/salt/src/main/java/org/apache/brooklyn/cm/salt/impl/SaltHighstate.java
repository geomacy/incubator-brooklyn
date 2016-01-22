/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.cm.salt.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Utility for handling a Salt highstate description.
 */
public class SaltHighstate {

    private static final Logger LOG = LoggerFactory.getLogger(SaltHighstate.class);
    public static TypeToken<Map<String, Object>> MODULE_METHOD_TYPE =
        new TypeToken<Map<String, Object>>() {};

    private SaltHighstate() {}

    public static void applyHighstate(String contents, Entity entity) {

        final String adaptedYaml = adaptForSaltYamlTypes(contents);
        LOG.debug("Parsing Salt highstate yaml:\n{}", adaptedYaml);
        final List<Object> objects = Yamls.parseAll(adaptedYaml);

        for (Object entry: objects) {
            final Map<String, Object> scopeMap = Yamls.getAs(entry, Map.class);
            applyStateScope(entity, scopeMap);
        }
    }

    private static void applyStateScope(Entity entity, Map<String, Object> scopeMap) {
        for (String scope: scopeMap.keySet()) {
            final Map<String, Object> stateMap = Yamls.getAs(scopeMap.get(scope), Map.class);
            for (String state: stateMap.keySet()) {
                applyStateSensors(state, stateMap.get(state), entity);
            }
        }
    }


    private static String adaptForSaltYamlTypes(String description) {
        return description.replaceAll("!!python/unicode", "!!java.lang.String");
    }

    @SuppressWarnings("unchecked")
    private static void applyStateSensors(String state, Object stateData, Entity entity) {
        addStateSensor(state, entity);
        Map<String, List<Object>> stateInfo = (Map<String, List<Object>>)stateData;
        for (String module : stateInfo.keySet()) {
            if (isSaltInternal(module)) {
                continue;
            }
            final List<Object> stateEntries = stateInfo.get(module);
            String method = "";
            Map<String, Object> moduleSettings = MutableMap.of();
            for (Object entry : stateEntries) {
                if (entry instanceof Map) {
                    moduleSettings.putAll((Map<String, Object>)entry);
                } else {
                    method = entry.toString();
                }
            }

            LOG.debug("Found {} state module {}", state, module + "."  +method);
            for (String name : moduleSettings.keySet()) {
                LOG.debug("    {} = {} ", name, moduleSettings.get(name).toString());
                addModuleSensors(entity, state, module, method, moduleSettings);
            }
        }
    }

    private static void addModuleSensors(Entity entity, String state, String module, String method,
             Map<String, Object> values) {

        String sensorName = Strings.join(ImmutableList.of(state, module, method), ".");

        final AttributeSensor<Map<String, Object>> newSensor =
            Sensors.newSensor(MODULE_METHOD_TYPE, sensorName,
                module + "." + method + " details for Salt state " + state);
        entity.sensors().set(newSensor, values);
    }

    private static void addStateSensor(String state, Entity entity) {
        List<String> states = entity.sensors().get(SaltEntityImpl.STATES);
        if (null == states || !states.contains(state)) {
            if (null == states) {
                states = MutableList.of();
            }
            states.add(state);
            entity.sensors().set(SaltEntityImpl.STATES, states);
        }
    }


    private static boolean isSaltInternal(String module) {
        return module.startsWith("__");
    }
}
