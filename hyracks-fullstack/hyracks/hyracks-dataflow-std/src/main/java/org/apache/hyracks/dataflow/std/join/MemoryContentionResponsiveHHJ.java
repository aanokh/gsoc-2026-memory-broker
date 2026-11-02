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

package org.apache.hyracks.dataflow.std.join;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePoolDynamicBudget;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;

public class MemoryContentionResponsiveHHJ extends OptimizedHybridHashJoin {

    private static final int MEMORY_REQUEST_FRAMES = 128;
    private static final int PROBE_BUFFER_PERCENT = 10;
    private DeallocatableFramePoolDynamicBudget framePool;

    public MemoryContentionResponsiveHHJ(IHyracksJobletContext jobletCtx, int memSizeInFrames, int numOfPartitions,
            String probeRelName, String buildRelName, RecordDescriptor probeRd, RecordDescriptor buildRd,
            ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc, IPredicateEvaluator probePredEval,
            IPredicateEvaluator buildPredEval, boolean isLeftOuter, IMissingWriterFactory[] nullWriterFactories1) {
        super(jobletCtx, memSizeInFrames, numOfPartitions, probeRelName, buildRelName, probeRd, buildRd, probeHpc,
                buildHpc, probePredEval, buildPredEval, isLeftOuter, nullWriterFactories1);
    }

    @Override
    public void initBuild() throws HyracksDataException {
        // one frame per partition is the least a join can run with, twice that gives it room to start
        int minFrames = Math.max(numOfPartitions, Math.min(2 * numOfPartitions, memSizeInFrames));
        framePool = new DeallocatableFramePoolDynamicBudget(jobletCtx,
                memSizeInFrames * jobletCtx.getInitialFrameSize(), minFrames);
        initBuildInternal(framePool);
    }

    @Override
    public void build(ByteBuffer buffer) throws HyracksDataException {
        adjustMemory(false);
        super.build(buffer);
    }

    @Override
    protected void processTupleBuildPhase(int tid, int pid) throws HyracksDataException {
        if (bufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
            return;
        }
        if (!spilledStatus.get(pid)) {
            int granted = framePool.requestMemory(MEMORY_REQUEST_FRAMES);
            if (granted > 0) {
                memSizeInFrames = framePool.getBudgetFrames();
            }
        }
        super.processTupleBuildPhase(tid, pid);
    }

    @Override
    public void closeBuild() throws HyracksDataException {
        adjustMemory(false);
        requestHashTableMemory();
        super.closeBuild();
        int heldFrames = framePool.getHeldFrames();
        int probeBufferFrames = Math.max(spilledStatus.cardinality(), heldFrames * PROBE_BUFFER_PERCENT / 100);
        framePool.releaseMemory(heldFrames + probeBufferFrames);
        memSizeInFrames = framePool.getBudgetFrames();
        reportUsage(true);
    }

    private void requestHashTableMemory() throws HyracksDataException {
        FreeSpace fs = calculateFreeSpace();
        if (fs.freeSpace < 0) {
            int frameSize = jobletCtx.getInitialFrameSize();
            int shortfallFrames = (int) ((-fs.freeSpace + frameSize - 1) / frameSize);
            int granted = framePool.requestMemory(shortfallFrames);
            if (granted > 0) {
                memSizeInFrames = framePool.getBudgetFrames();
            }
        }
    }

    @Override
    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        adjustMemory(true);
        super.probe(buffer, writer);
    }

    private void adjustMemory(boolean isProbing) throws HyracksDataException {
        int framesToSpill = framePool.syncBudgetWithBroker();
        memSizeInFrames = framePool.getBudgetFrames();
        if (framesToSpill > 0) {
            if (isProbing) {
                spillProbePhase(framesToSpill);
            } else {
                spillBuildPhase(framesToSpill);
            }
            framePool.acknowledgeBudget();
        }
        reportUsage(isProbing);
    }

    private void reportUsage(boolean isProbing) {
        int heldFrames = framePool.getHeldFrames();
        if (isProbing) {
            framePool.reportUsage(0, 0, heldFrames);
        } else {
            framePool.reportUsage(0, heldFrames, 0);
        }
    }

    private void spillBuildPhase(int framesToSpill) throws HyracksDataException {
        int freedFrames = 0;
        while (freedFrames < framesToSpill) {
            int victim = spillPolicy.findSpilledPartitionWithMaxMemoryUsage();
            if (victim < 0) {
                victim = spillPolicy.findInMemPartitionWithMaxMemoryUsage();
            }
            if (victim < 0) {
                break;
            }
            freedFrames += bufferManager.getPhysicalSize(victim) / jobletCtx.getInitialFrameSize();
            spillPartition(victim);
        }
    }

    private void spillProbePhase(int framesToSpill) throws HyracksDataException {
        int freedFrames = 0;
        while (freedFrames < framesToSpill || calculateFreeSpace().freeSpace < 0) {
            int victim = spillPolicy.findInMemPartitionWithMaxMemoryUsage();
            if (victim < 0 || buildPSizeInTups[victim] <= 0) {
                break;
            }
            freedFrames += bufferManager.getPhysicalSize(victim) / jobletCtx.getInitialFrameSize();
            spillPartition(victim);
            closeBuildPartition(victim);
        }
        if (freedFrames > 0) {
            rebuildHashTable();
        }
    }

    private void rebuildHashTable() throws HyracksDataException {
        FreeSpace fs = calculateFreeSpace();
        table.close();
        table = new SerializableHashTable(fs.tuplesInMemory, jobletCtx, bufferManagerForHashTable);
        inMemJoiner.resetTable(table);
        buildHashTable();
    }
}
