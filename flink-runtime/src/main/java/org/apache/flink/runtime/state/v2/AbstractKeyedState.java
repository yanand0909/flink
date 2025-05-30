/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.v2;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.internal.InternalKeyedState;

/**
 * The {@code AbstractKeyedState} is the root of the abstract state implementation hierarchy,
 * similar to the {@link State} being the root of the public API state hierarchy.
 *
 * <p>The public API state hierarchy is intended to be programmed against by Flink applications. The
 * internal state hierarchy holds all the auxiliary methods that communicates with {@link
 * AsyncExecutionController} and not intended to be used by user applications.
 *
 * @param <K> The type of key the state is associated to.
 * @param <V> The type of values kept internally in state.
 */
@Internal
public abstract class AbstractKeyedState<K, N, V> implements InternalKeyedState<K, N, V> {

    protected final StateRequestHandler stateRequestHandler;

    private final StateDescriptor<V> stateDescriptor;

    private final ThreadLocal<TypeSerializer<V>> valueSerializer;

    /**
     * Creates a new AbstractKeyedState with the given asyncExecutionController and stateDescriptor.
     */
    public AbstractKeyedState(
            StateRequestHandler stateRequestHandler, StateDescriptor<V> stateDescriptor) {
        this.stateRequestHandler = stateRequestHandler;
        this.stateDescriptor = stateDescriptor;
        this.valueSerializer = ThreadLocal.withInitial(stateDescriptor::getSerializer);
    }

    /**
     * Submit a state request to AEC.
     *
     * @param stateRequestType the type of this request.
     * @param payload the payload input for this request.
     * @return the state future.
     */
    protected final <IN, OUT> StateFuture<OUT> handleRequest(
            StateRequestType stateRequestType, IN payload) {
        return stateRequestHandler.handleRequest(this, stateRequestType, payload);
    }

    protected final <IN, OUT> OUT handleRequestSync(StateRequestType stateRequestType, IN payload) {
        return stateRequestHandler.handleRequestSync(this, stateRequestType, payload);
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        stateRequestHandler.setCurrentNamespaceForState(this, namespace);
    }

    @Override
    public final StateFuture<Void> asyncClear() {
        return handleRequest(StateRequestType.CLEAR, null);
    }

    public final void clear() {
        handleRequestSync(StateRequestType.CLEAR, null);
    }

    /** Return specific {@code StateDescriptor}. */
    public StateDescriptor<V> getStateDescriptor() {
        return stateDescriptor;
    }

    /** Return related value serializer. */
    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer.get();
    }

    public StateRequestHandler getStateRequestHandler() {
        return stateRequestHandler;
    }
}
