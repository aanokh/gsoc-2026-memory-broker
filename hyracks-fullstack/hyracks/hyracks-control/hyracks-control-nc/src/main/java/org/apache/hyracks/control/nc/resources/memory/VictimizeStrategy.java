/*
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
package org.apache.hyracks.control.nc.resources.memory;

import java.util.Collection;
import java.util.Map;

import org.apache.hyracks.api.resources.memory.OperatorMemoryHandle;
import org.apache.hyracks.control.nc.resources.memory.ResourceBroker.OperatorEntry;

public class VictimizeStrategy implements IBrokerStrategy {

    @Override
    public boolean isOperatorAllowedToGrow(OperatorEntry requester, int frames, ResourceBroker broker) {
        return true;
    }

    @Override
    public Map<OperatorMemoryHandle, Integer> selectVictims(OperatorEntry requester, int framesNeeded,
            Collection<OperatorEntry> operators, ResourceBroker broker) {
        int fairShareFrames = broker.getFairShareFrames();
        OperatorEntry victim = null;
        for (OperatorEntry operator : operators) {
            if (operator.getJobId().equals(requester.getJobId()) || operator.isWaitingForMemory()
                    || operator.getOwedFrames() > 0) {
                continue;
            }
            OperatorMemoryHandle handle = operator.getHandle();
            int framesAboveFairShare = operator.getBudgetFrames() - fairShareFrames;
            int takeableFrames = Math.min(framesAboveFairShare, handle.getEasyFrames() + handle.getMediumFrames());
            if (takeableFrames >= framesNeeded
                    && (victim == null || operator.getBudgetFrames() > victim.getBudgetFrames())) {
                victim = operator;
            }
        }
        if (victim == null) {
            return Map.of();
        }
        return Map.of(victim.getHandle(), framesNeeded);
    }

    @Override
    public Map<OperatorMemoryHandle, Integer> selectVictimsAfterGrant(OperatorEntry requester,
            Collection<OperatorEntry> operators, ResourceBroker broker) {
        return Map.of();
    }

    @Override
    public boolean shouldRequesterWait(OperatorEntry requester, int frames, ResourceBroker broker) {
        return true;
    }
}
