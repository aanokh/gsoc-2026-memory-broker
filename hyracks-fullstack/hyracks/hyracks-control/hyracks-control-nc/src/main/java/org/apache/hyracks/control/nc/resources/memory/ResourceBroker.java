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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;

import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.resources.memory.IResourceBroker;
import org.apache.hyracks.api.resources.memory.OperatorMemoryHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResourceBroker implements IResourceBroker {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long MEMORY_WAIT_TIMEOUT_MILLIS = 5000;

    // operators that can grow and be cut, and operators with a fixed budget
    private final Map<OperatorMemoryHandle, OperatorEntry> operators = new LinkedHashMap<>();
    private final Map<OperatorMemoryHandle, OperatorEntry> staticOperators = new HashMap<>();

    // operators waiting to grow and new jobs waiting to start, arrivals are always served first
    private final Deque<OperatorEntry> requestorLine = new ArrayDeque<>();
    private final Deque<Admission> arrivalLine = new ArrayDeque<>();

    // frames each job set aside on this node, and jobs still setting theirs aside
    private final Map<JobId, Integer> queryReserves = new HashMap<>();
    private final Set<JobId> openingQueries = new HashSet<>();

    private final IBrokerStrategy strategy;
    private final int totalFrames;
    private final int fairShareFrames;
    private int freeFrames;

    public ResourceBroker(int totalFrames, int multiprogrammingLevel, IBrokerStrategy strategy) {
        this.totalFrames = totalFrames;
        this.fairShareFrames = totalFrames / multiprogrammingLevel;
        this.freeFrames = totalFrames;
        this.strategy = strategy;
        LOGGER.info("resource broker started with {} frames, fair share {} frames", totalFrames, fairShareFrames);
    }

    // registers an operator that can grow and be cut, starting at its min
    @Override
    public synchronized OperatorMemoryHandle registerOperator(IHyracksJobletContext ctx, int minFrames)
            throws HyracksDataException {
        takeMinFramesFromReserveOrFree(ctx, minFrames);

        OperatorMemoryHandle handle = new OperatorMemoryHandle(minFrames);
        OperatorEntry entry = new OperatorEntry(handle, ctx.getJobId(), minFrames);
        operators.put(handle, entry);
        return handle;
    }

    // removes an operator and gives back everything it held
    @Override
    public synchronized void deregisterOperator(OperatorMemoryHandle handle) {
        OperatorEntry entry = operators.remove(handle);
        if (entry != null) {
            returnFramesToReserveAndFree(entry);
            requestorLine.remove(entry);
            notifyAll();
        }
    }

    // registers an operator with a fixed budget that is never cut
    @Override
    public synchronized OperatorMemoryHandle registerStaticOperator(IHyracksJobletContext ctx, int frames)
            throws HyracksDataException {
        takeMinFramesFromReserveOrFree(ctx, frames);

        OperatorMemoryHandle handle = new OperatorMemoryHandle(frames);
        OperatorEntry entry = new OperatorEntry(handle, ctx.getJobId(), frames);
        staticOperators.put(handle, entry);
        return handle;
    }

    // removes a fixed budget operator and gives back its frames
    @Override
    public synchronized void deregisterStaticOperator(OperatorMemoryHandle handle) {
        OperatorEntry entry = staticOperators.remove(handle);
        if (entry != null) {
            returnFramesToReserveAndFree(entry);
            notifyAll();
        }
    }

    // gives an operator more frames, takes them from victims, or refuses
    @Override
    public synchronized int requestMemory(OperatorMemoryHandle handle, int frames) throws HyracksDataException {
        OperatorEntry entry = operators.get(handle);
        // an operator that still owes frames from a cut has to give them back before it can grow
        if (entry == null || frames <= 0 || entry.owedFrames > 0
                || !strategy.isOperatorAllowedToGrow(entry, frames, this)) {
            return 0;
        }

        // nobody is waiting and it fits, so grant right away
        if (arrivalLine.isEmpty() && requestorLine.isEmpty() && freeFrames >= frames) {
            grantMemory(entry, frames);
            return frames;
        }

        // not enough, so let the strategy pick victims for what is missing
        int framesNeeded = frames + framesWaitingRequestors() + framesWaitingArrivals() - freeFrames - getOwedFrames();
        Map<OperatorMemoryHandle, Integer> victims = Map.of();
        if (framesNeeded > 0) {
            victims = strategy.selectVictims(entry, framesNeeded, operators.values(), this);
            takeFromVictims(entry, victims);
        }

        // no victims or the strategy says no waiting, so the operator spills on its own
        if (victims.isEmpty() || !strategy.shouldRequesterWait(entry, frames, this)) {
            return 0;
        }
        return waitForMemory(entry, frames);
    }

    // operator voluntarily lowers its own budget
    @Override
    public synchronized void releaseMemory(OperatorMemoryHandle handle, int newBudgetFrames) {
        OperatorEntry entry = operators.get(handle);
        if (entry != null) {
            lowerBudget(entry, newBudgetFrames);
        }
    }

    // operator reports what it holds, so owed frames it no longer holds become free
    @Override
    public synchronized void acknowledgeBudget(OperatorMemoryHandle handle, int heldFrames) {
        OperatorEntry entry = operators.get(handle);
        if (entry == null) {
            return;
        }
        int stillOwed = Math.min(entry.owedFrames, Math.max(0, heldFrames - entry.budgetFrames));
        freeFrames += entry.owedFrames - stillOwed;
        entry.owedFrames = stillOwed;
        notifyAll();
    }

    public int getTotalFrames() {
        return totalFrames;
    }

    public int getFairShareFrames() {
        return fairShareFrames;
    }

    public synchronized int getFreeFrames() {
        return freeFrames;
    }

    // frames cut from operators that they have not given back yet
    public synchronized int getOwedFrames() {
        int frames = 0;
        for (OperatorEntry entry : operators.values()) {
            frames += entry.owedFrames;
        }
        return frames;
    }

    // takes an operator's min from its job's reserve, or from free memory if the job has no reserve
    private void takeMinFramesFromReserveOrFree(IHyracksJobletContext ctx, int minFrames) throws HyracksDataException {
        JobId jobId = ctx.getJobId();
        int queryReserveFrames = getQueryReserveFrames(ctx);
        if (queryReserveFrames > 0) {
            ensureQueryReserve(ctx, queryReserveFrames);
        }

        Integer reserveFrames = queryReserves.get(jobId);
        if (reserveFrames == null) {
            waitForAdmission(minFrames);
            freeFrames -= minFrames;
        } else if (reserveFrames < minFrames) {
            throw HyracksDataException.create(ErrorCode.INSUFFICIENT_MEMORY);
        } else {
            queryReserves.put(jobId, reserveFrames - minFrames);
        }
    }

    // gives an operator's min back to its job's reserve and the rest to free memory
    private void returnFramesToReserveAndFree(OperatorEntry entry) {
        int returnedFrames = entry.budgetFrames + entry.owedFrames;
        Integer reserveFrames = queryReserves.get(entry.jobId);
        if (reserveFrames != null) {
            queryReserves.put(entry.jobId, reserveFrames + entry.minFrames);
            returnedFrames -= entry.minFrames;
        }
        freeFrames += returnedFrames;
    }

    // frames this job asked to set aside on this node, or 0 if none
    private static int getQueryReserveFrames(IHyracksJobletContext ctx) {
        ActivityClusterGraph acg = ctx.getActivityClusterGraph();
        if (acg == null || acg.getRequiredNodeMemory() == null) {
            return 0;
        }
        Long reserveBytes = acg.getRequiredNodeMemory().get(ctx.getServiceContext().getNodeId());
        if (reserveBytes == null) {
            return 0;
        }
        return (int) Math.min(Integer.MAX_VALUE, reserveBytes / ctx.getInitialFrameSize());
    }

    // sets aside the job's reserve once, other operators of the same job wait for it
    private void ensureQueryReserve(IHyracksJobletContext ctx, int reserveFrames) throws HyracksDataException {
        JobId jobId = ctx.getJobId();

        // another operator of this job is already setting it aside
        try {
            while (openingQueries.contains(jobId)) {
                wait();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        }
        if (queryReserves.containsKey(jobId)) {
            return;
        }

        // this operator sets it aside and releases it when the job ends
        openingQueries.add(jobId);
        try {
            waitForAdmission(reserveFrames);
        } finally {
            openingQueries.remove(jobId);
            notifyAll();
        }
        freeFrames -= reserveFrames;
        queryReserves.put(jobId, reserveFrames);
        ctx.registerDeallocatable(() -> releaseQueryReserve(jobId));
    }

    // returns whatever is left of a job's reserve when the job ends
    private synchronized void releaseQueryReserve(JobId jobId) {
        Integer reserveFrames = queryReserves.remove(jobId);
        if (reserveFrames != null) {
            freeFrames += reserveFrames;
            notifyAll();
        }
    }

    // waits until minFrames are free and nobody is ahead in the arrival line
    private void waitForAdmission(int minFrames) throws HyracksDataException {
        if (minFrames > totalFrames) {
            throw HyracksDataException.create(ErrorCode.INSUFFICIENT_MEMORY);
        }

        Admission admission = new Admission(minFrames);
        arrivalLine.addLast(admission);
        try {
            // cut frames that operators hold above their min to make room
            int framesNeeded = framesWaitingArrivals() - freeFrames - getOwedFrames();
            if (framesNeeded > 0) {
                takeFromVictims(null, selectAdmissionVictims(framesNeeded));
            }

            while (arrivalLine.peekFirst() != admission || freeFrames < minFrames) {
                wait();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        } finally {
            arrivalLine.remove(admission);
            notifyAll();
        }
    }

    // waits up to the timeout for the requested frames, first come first served
    private int waitForMemory(OperatorEntry entry, int frames) throws HyracksDataException {
        entry.framesWaitingFor = frames;
        requestorLine.addLast(entry);

        long deadline = System.currentTimeMillis() + MEMORY_WAIT_TIMEOUT_MILLIS;
        try {
            while (!arrivalLine.isEmpty() || requestorLine.peekFirst() != entry || freeFrames < frames) {
                long remainingMillis = deadline - System.currentTimeMillis();
                if (remainingMillis <= 0 || !operators.containsKey(entry.handle)) {
                    return 0;
                }
                wait(remainingMillis);
            }
            grantMemory(entry, frames);
            return frames;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        } finally {
            entry.framesWaitingFor = 0;
            requestorLine.remove(entry);
            notifyAll();
        }
    }

    // gives free frames to the operator, then asks the strategy if anyone else should be cut
    private void grantMemory(OperatorEntry entry, int frames) {
        freeFrames -= frames;
        entry.budgetFrames += frames;
        entry.handle.setBudgetFrames(entry.budgetFrames);

        takeFromVictims(entry, strategy.selectVictimsAfterGrant(entry, operators.values(), this));
    }

    // frames the requestor line is waiting for
    private int framesWaitingRequestors() {
        int frames = 0;
        for (OperatorEntry waiter : requestorLine) {
            frames += waiter.framesWaitingFor;
        }
        return frames;
    }

    // frames the arrival line is waiting for
    private int framesWaitingArrivals() {
        int frames = 0;
        for (Admission admission : arrivalLine) {
            frames += admission.minFrames;
        }
        return frames;
    }

    // picks frames above min to cut for an admission, easy first then medium, largest first
    private Map<OperatorMemoryHandle, Integer> selectAdmissionVictims(int framesNeeded) {
        List<OperatorEntry> candidates = new ArrayList<>();
        for (OperatorEntry entry : operators.values()) {
            if (entry.framesAboveMin() > 0) {
                candidates.add(entry);
            }
        }
        candidates.sort(Comparator.comparingInt(OperatorEntry::framesAboveMin).reversed());

        Map<OperatorMemoryHandle, Integer> victims = new HashMap<>();
        int stillNeeded = takeFramesAboveMin(candidates, framesNeeded, OperatorMemoryHandle::getEasyFrames, victims);
        takeFramesAboveMin(candidates, stillNeeded, OperatorMemoryHandle::getMediumFrames, victims);
        return victims;
    }

    // takes up to framesNeeded from the candidates using one grade, returns what is still missing
    private static int takeFramesAboveMin(List<OperatorEntry> candidates, int framesNeeded,
            ToIntFunction<OperatorMemoryHandle> takeableFrames, Map<OperatorMemoryHandle, Integer> victims) {
        for (OperatorEntry candidate : candidates) {
            if (framesNeeded <= 0) {
                break;
            }
            int framesLeft = candidate.framesAboveMin() - victims.getOrDefault(candidate.handle, 0);
            int takenFrames = Math.min(framesNeeded, Math.min(takeableFrames.applyAsInt(candidate.handle), framesLeft));
            if (takenFrames > 0) {
                victims.merge(candidate.handle, takenFrames, Integer::sum);
                framesNeeded -= takenFrames;
            }
        }
        return framesNeeded;
    }

    // cuts each victim's budget by the given frames
    private void takeFromVictims(OperatorEntry requester, Map<OperatorMemoryHandle, Integer> victims) {
        for (Map.Entry<OperatorMemoryHandle, Integer> victim : victims.entrySet()) {
            OperatorEntry entry = operators.get(victim.getKey());
            if (entry != null && entry != requester && victim.getValue() > 0) {
                lowerBudget(entry, entry.budgetFrames - victim.getValue());
            }
        }
    }

    // lowers a budget but never below min, the cut frames are owed until the operator acks
    private void lowerBudget(OperatorEntry entry, int newBudgetFrames) {
        int newBudget = Math.max(newBudgetFrames, entry.minFrames);
        if (newBudget < entry.budgetFrames) {
            entry.owedFrames += entry.budgetFrames - newBudget;
            entry.budgetFrames = newBudget;
            entry.handle.setBudgetFrames(newBudget);
        }
    }

    // what the broker knows about one operator
    public static final class OperatorEntry {
        private final OperatorMemoryHandle handle;
        private final JobId jobId;
        private final int minFrames;
        private int budgetFrames;
        private int owedFrames;
        private int framesWaitingFor;

        private OperatorEntry(OperatorMemoryHandle handle, JobId jobId, int minFrames) {
            this.handle = handle;
            this.jobId = jobId;
            this.minFrames = minFrames;
            this.budgetFrames = minFrames;
        }

        private int framesAboveMin() {
            return budgetFrames - minFrames;
        }

        public OperatorMemoryHandle getHandle() {
            return handle;
        }

        public JobId getJobId() {
            return jobId;
        }

        public int getMinFrames() {
            return minFrames;
        }

        public int getBudgetFrames() {
            return budgetFrames;
        }

        public int getOwedFrames() {
            return owedFrames;
        }

        public boolean isWaitingForMemory() {
            return framesWaitingFor > 0;
        }
    }

    // a place in the arrival line
    private static final class Admission {
        private final int minFrames;

        private Admission(int minFrames) {
            this.minFrames = minFrames;
        }
    }
}
