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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.resources.memory.IResourceBroker;
import org.apache.hyracks.api.resources.memory.OperatorMemoryHandle;

public class VariableFramePoolStaticBudget extends VariableFramePool {

    private final IResourceBroker broker;
    private final OperatorMemoryHandle handle;

    public VariableFramePoolStaticBudget(IHyracksTaskContext ctx, int memBudgetInBytes) throws HyracksDataException {
        super(ctx, memBudgetInBytes);
        broker = ctx.getJobletContext().getServiceContext().getResourceBroker();
        if (broker != null) {
            OperatorMemoryHandle registered =
                    broker.registerStaticOperator(ctx.getJobletContext(), memBudgetInBytes / ctx.getInitialFrameSize());
            // released at job end, not task end, since frames can be handed to the next task
            ctx.getJobletContext().registerDeallocatable(() -> broker.deregisterStaticOperator(registered));
            handle = registered;
        } else {
            handle = null;
        }
    }

    @Override
    public void close() {
        super.close();
        if (handle != null) {
            broker.deregisterStaticOperator(handle);
        }
    }
}
