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
package org.apache.hyracks.dataflow.std.buffermanager;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.resources.memory.IResourceBroker;
import org.apache.hyracks.api.resources.memory.OperatorMemoryHandle;

public class DeallocatableFramePoolDynamicBudget extends DeallocatableFramePool {

    private final IResourceBroker broker;
    private final OperatorMemoryHandle handle;
    private int acknowledgedBudget;

    public DeallocatableFramePoolDynamicBudget(IHyracksJobletContext ctx, int memBudgetInBytes, int minFrames)
            throws HyracksDataException {
        super(ctx, memBudgetInBytes);
        broker = ctx.getServiceContext().getResourceBroker();
        if (broker != null) {
            OperatorMemoryHandle registered = broker.registerOperator(ctx, minFrames);
            ctx.registerDeallocatable(() -> broker.deregisterOperator(registered));
            handle = registered;
            memBudget = toBytes(handle.getBudgetFrames());
            acknowledgedBudget = memBudget;
        } else {
            handle = null;
        }
    }

    public int syncBudgetWithBroker() {
        if (handle == null) {
            return 0;
        }
        memBudget = toBytes(handle.getBudgetFrames());
        while (allocated > memBudget && !buffers.isEmpty()) {
            ByteBuffer idle = buffers.removeFirst();
            ctx.deallocateFrames(idle.capacity());
            allocated -= idle.capacity();
        }
        int framesToSpill = toFrames(Math.max(0, allocated - memBudget));
        if (framesToSpill == 0 && memBudget != acknowledgedBudget) {
            acknowledgeBudget();
        }
        return framesToSpill;
    }

    public int requestMemory(int frames) throws HyracksDataException {
        if (handle == null) {
            return 0;
        }
        int granted = broker.requestMemory(handle, frames);
        memBudget = toBytes(handle.getBudgetFrames());
        return granted;
    }

    public void releaseMemory(int newBudgetFrames) {
        if (handle != null && newBudgetFrames < getBudgetFrames()) {
            broker.releaseMemory(handle, newBudgetFrames);
            memBudget = toBytes(handle.getBudgetFrames());
            acknowledgeBudget();
        }
    }

    public void acknowledgeBudget() {
        if (handle != null) {
            acknowledgedBudget = memBudget;
            broker.acknowledgeBudget(handle, toFrames(allocated));
        }
    }

    public void reportUsage(int easyFrames, int mediumFrames, int hardFrames) {
        if (handle != null) {
            int freeFrames = toFrames(Math.max(0, memBudget - allocated)) + buffers.size();
            handle.setFrames(easyFrames + freeFrames, mediumFrames, hardFrames);
        }
    }

    public int getHeldFrames() {
        return toFrames(allocated) - buffers.size();
    }

    public int getBudgetFrames() {
        return toFrames(memBudget);
    }

    private int toFrames(int bytes) {
        return bytes / ctx.getInitialFrameSize();
    }

    private int toBytes(int frames) {
        int frameSize = ctx.getInitialFrameSize();
        return Math.min(frames, Integer.MAX_VALUE / frameSize) * frameSize;
    }

    @Override
    public void deAllocateBuffer(ByteBuffer buffer) {
        if (buffer.capacity() != ctx.getInitialFrameSize() || allocated > memBudget) {
            ctx.deallocateFrames(buffer.capacity());
            allocated -= buffer.capacity();
        } else {
            buffers.add(buffer);
        }
    }

    @Override
    public void close() {
        super.close();
        if (handle != null) {
            broker.deregisterOperator(handle);
        }
    }
}
