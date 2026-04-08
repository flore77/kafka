/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import org.apache.kafka.common.memory.MemoryPool;

import java.nio.ByteBuffer;

/**
 * A ref-counted wrapper around a {@link ByteBuffer} allocated from a {@link MemoryPool}.
 * When multiple consumers share the same underlying buffer (e.g. multiple partitions
 * in a single fetch response), each holder calls {@link #release()} when done.
 * The buffer is returned to the pool when the last holder releases.
 *
 * <p>Not thread-safe — intended for single consumer thread use.</p>
 */
public class PooledBuffer {

    public static final PooledBuffer NONE = new PooledBuffer(null, MemoryPool.NONE, 0) {
        @Override
        public void release() {
            // no-op
        }

        @Override
        public void forceRelease() {
            // no-op
        }
    };

    private final ByteBuffer buffer;
    private final MemoryPool pool;
    private int refCount;

    public PooledBuffer(ByteBuffer buffer, MemoryPool pool, int refCount) {
        this.buffer = buffer;
        this.pool = pool;
        this.refCount = refCount;
    }

    public void release() {
        if (--refCount == 0) {
            pool.release(buffer);
        }
    }

    /**
     * Immediately releases the buffer back to the pool, regardless of the ref count.
     * Used for error/cleanup paths where all outstanding references are being discarded.
     */
    public void forceRelease() {
        if (refCount > 0) {
            refCount = 0;
            pool.release(buffer);
        }
    }
}
