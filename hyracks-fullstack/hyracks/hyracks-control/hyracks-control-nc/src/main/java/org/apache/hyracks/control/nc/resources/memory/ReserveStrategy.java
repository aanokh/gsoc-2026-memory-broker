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

public class ReserveStrategy extends VictimizeStrategy {

    private final int reservePercent;

    public ReserveStrategy(int reservePercent) {
        if (reservePercent < 0 || reservePercent >= 100) {
            throw new IllegalArgumentException("reserve percent must be between 0 and 99: " + reservePercent);
        }
        this.reservePercent = reservePercent;
    }

    @Override
    public Map<OperatorMemoryHandle, Integer> selectVictimsAfterGrant(OperatorEntry requester,
            Collection<OperatorEntry> operators, ResourceBroker broker) {
        int reserveFrames = (int) ((long) broker.getTotalFrames() * reservePercent / 100);
        int missingFrames = reserveFrames - broker.getFreeFrames() - broker.getOwedFrames();
        if (missingFrames <= 0) {
            return Map.of();
        }
        return selectVictims(requester, missingFrames, operators, broker);
    }

    @Override
    public boolean shouldRequesterWait(OperatorEntry requester, int frames, ResourceBroker broker) {
        return false;
    }
}
