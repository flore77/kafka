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
package org.apache.kafka.common.memory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * A size-bucketed memory pool that recycles {@link ByteBuffer}s.
 *
 * <p>Requested sizes are rounded up to the next power of 2 (minimum 1024).
 * On {@link #release(ByteBuffer)}, the buffer is returned to the free list for its bucket
 * and can be reused by a subsequent {@link #tryAllocate(int)} of the same or smaller size.
 *
 * <p>Not thread-safe — intended for single consumer thread use.
 */
public class RecyclingMemoryPool implements MemoryPool {

    private static final int MIN_BUCKET = 1024;

    private final Map<Integer, Deque<ByteBuffer>> freeLists = new HashMap<>();

    @Override
    public ByteBuffer tryAllocate(int sizeBytes) {
        if (sizeBytes <= 0)
            throw new IllegalArgumentException("requested size " + sizeBytes + " <= 0");

        int bucket = bucket(sizeBytes);
        Deque<ByteBuffer> freeList = freeLists.get(bucket);

        ByteBuffer buffer;
        if (freeList != null && (buffer = freeList.poll()) != null) {
            buffer.clear();
            buffer.limit(sizeBytes);
            return buffer;
        }

        buffer = ByteBuffer.allocate(bucket);
        buffer.limit(sizeBytes);
        return buffer;
    }

    @Override
    public void release(ByteBuffer previouslyAllocated) {
        if (previouslyAllocated == null)
            throw new IllegalArgumentException("provided null buffer");

        int bucket = previouslyAllocated.capacity();
        freeLists.computeIfAbsent(bucket, k -> new ArrayDeque<>()).offer(previouslyAllocated);
    }

    @Override
    public long size() {
        return Long.MAX_VALUE;
    }

    @Override
    public long availableMemory() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean isOutOfMemory() {
        return false;
    }

    static int bucket(int size) {
        if (size <= MIN_BUCKET) return MIN_BUCKET;
        return Integer.highestOneBit(size - 1) << 1;
    }
}
