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

import org.apache.brooklyn.cm.salt.SaltConfig;
import org.apache.brooklyn.cm.salt.SaltEntity;
import org.apache.brooklyn.entity.stock.EffectorStartableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;

@Beta
public class SaltEntityImpl extends EffectorStartableImpl implements SaltEntity {
    private static final Logger LOG = LoggerFactory.getLogger(SaltEntityImpl.class);

    public SaltEntityImpl() {
        super();
    }

    @Override
    public void init() {
        super.init();
        
        SaltConfig.SaltMode mode = getConfig(SaltConfig.SALT_MODE);
        LOG.info("Initialize SaltStack {} mode", mode.name());
        new SaltLifecycleEffectorTasks().attachLifecycleEffectors(this);
    }

    @Override
    public void populateServiceNotUpDiagnostics() {
        // TODO: noop for now;
    }

}
