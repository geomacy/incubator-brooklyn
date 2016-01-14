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
package org.apache.brooklyn.cm.salt;

import java.io.InputStream;
import java.util.Map;

import org.apache.brooklyn.cm.salt.impl.SaltEntityImpl;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.entity.software.base.AbstractSoftwareProcessDriver;

// TODO: does this belong to the _.impl package?
public class SaltEntitySimulatedDriver extends AbstractSoftwareProcessDriver implements SaltEntityDriver {

    public SaltEntitySimulatedDriver(SaltEntityImpl entity, SimulatedLocation machine) {
        super(entity, machine);
    }

    @Override
    public SaltEntityImpl getEntity() {
        return (SaltEntityImpl) super.getEntity();
    }

	@Override
	public boolean isRunning() {
		return true;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void runPreInstallCommand() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void install() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void runPostInstallCommand() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void customize() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void runPreLaunchCommand() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void launch() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void runPostLaunchCommand() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void createDirectory(String directoryName, String summaryForLogging) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int copyResource(Map<Object, Object> sshFlags, String source, String target, boolean createParentDir) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int copyResource(Map<Object, Object> sshFlags, InputStream source, String target, boolean createParentDir) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getRunDir() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getInstallDir() {
		// TODO Auto-generated method stub
		return null;
	}

}
