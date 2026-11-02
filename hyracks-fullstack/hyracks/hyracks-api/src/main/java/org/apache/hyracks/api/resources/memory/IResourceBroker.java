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
package org.apache.hyracks.api.resources.memory;

import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Hands out operator memory on a node in frames and moves it between running operators.
 */
public interface IResourceBroker {

    /**
     * Registers an operator whose budget can grow and be cut. It starts at, and is never cut below, minFrames.
     * Blocks until the frames are available.
     */
    OperatorMemoryHandle registerOperator(IHyracksJobletContext ctx, int minFrames) throws HyracksDataException;

    /**
     * Removes the operator and returns all of its frames.
     */
    void deregisterOperator(OperatorMemoryHandle handle);

    /**
     * Registers an operator with a fixed budget that is never cut. Blocks until the frames are available.
     */
    OperatorMemoryHandle registerStaticOperator(IHyracksJobletContext ctx, int frames) throws HyracksDataException;

    /**
     * Removes a fixed budget operator and returns its frames.
     */
    void deregisterStaticOperator(OperatorMemoryHandle handle);

    /**
     * Asks for more frames, which may cut other operators and wait for them.
     *
     * @return the frames granted or 0 if denied
     */
    int requestMemory(OperatorMemoryHandle handle, int frames) throws HyracksDataException;

    /**
     * Lowers the operator's own budget voluntarily.
     */
    void releaseMemory(OperatorMemoryHandle handle, int newBudgetFrames);

    /**
     * Reports the frames the operator still holds after its budget was cut, so the frames it gave up can be
     * used by the broker.
     */
    void acknowledgeBudget(OperatorMemoryHandle handle, int heldFrames);
}
