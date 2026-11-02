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

/**
 * Decides who gives up memory when the broker runs short.
 */
public interface IBrokerStrategy {

    /**
     * Decides whether the requester may get more frames at all, before anything is granted.
     */
    boolean isOperatorAllowedToGrow(OperatorEntry requester, int frames, ResourceBroker broker);

    /**
     * Picks operators to cut when free memory cannot cover a request.
     *
     * @return the frames to take from each victim, or an empty map for none
     */
    Map<OperatorMemoryHandle, Integer> selectVictims(OperatorEntry requester, int framesNeeded,
            Collection<OperatorEntry> operators, ResourceBroker broker);

    /**
     * Picks operators to cut after a grant.
     *
     * @return the frames to take from each victim, or an empty map for none
     */
    Map<OperatorMemoryHandle, Integer> selectVictimsAfterGrant(OperatorEntry requester,
            Collection<OperatorEntry> operators, ResourceBroker broker);

    /**
     * Decides whether the requester waits for its victims to give memory back, or gives up and spills.
     */
    boolean shouldRequesterWait(OperatorEntry requester, int frames, ResourceBroker broker);
}
