/*
 * DirectRangeIterator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb;

import com.apple.foundationdb.async.AsyncIterator;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@NotThreadSafe
class DirectRangeIterator implements AsyncIterator<KeyValue> {
    static final Queue<DirectBufferIterator> BUFFERS = new ArrayBlockingQueue<>(64); // BoundedConcurrentQueueService
    private final Queue<CompletableFuture<DirectBufferIterator>> futures = new ArrayBlockingQueue<>(2);
    private final FDBTransaction tr;
    private KeySelector begin;
    private KeySelector end;
    private final boolean snapshot;
    private final int rowLimit;
    private int rowsRemaining;
    private final boolean reverse;
    private final StreamingMode streamingMode;
    private DirectBufferIterator currentIterator;
    private boolean exhausted;
    private boolean isCancelled;
    private byte[] prevKey = null;

    DirectRangeIterator(FDBTransaction transaction, boolean isSnapshot,
                                KeySelector begin, KeySelector end, int rowLimit,
                                boolean reverse, StreamingMode streamingMode) {
        this.tr = transaction;
        this.begin = begin;
        this.end = end;
        this.snapshot = isSnapshot;
        this.rowLimit = rowLimit;
        this.rowsRemaining = rowLimit;
        this.reverse = reverse;
        this.streamingMode = streamingMode;
        fetchNext();
        }

        private void fetchNext() {
            futures.offer(CompletableFuture.supplyAsync(() -> {
                DirectBufferIterator it = BUFFERS.poll();
                // DOIT ... (it.buffer());
                return it;
            }, tr.getExecutor()));
        }

        @Override
        public CompletableFuture<Boolean> onHasNext() {
            return CompletableFuture.supplyAsync(() -> {
                while (true) {
                    if (currentIterator == null) {
                        try {
                            currentIterator = futures.poll().get();
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    }
                    if (currentIterator.isNew()) {
                        currentIterator.reset();
                        if (rowsRemaining <= currentIterator.count()) {
                            if (reverse) {
                                end = KeySelector.firstGreaterOrEqual(currentIterator.lastKey());
                            } else {
                                begin = KeySelector.firstGreaterThan(currentIterator.lastKey());
                            }
                            fetchNext();
                        }
                    }
                    if (currentIterator.hasNext()) {
                        rowsRemaining--;
                        return true;
                    } else {
                        currentIterator.reset();
                        BUFFERS.offer(currentIterator);
                        currentIterator = null;
                        futures.remove();
                    }
                }
            }, tr.getExecutor());
        }

        @Override
        public boolean hasNext() {
            return onHasNext().join();
        }

        @Override
        public KeyValue next() {
            return currentIterator == null? null: currentIterator.next();
        }

        @Override
        public void cancel() {
            this.isCancelled = true;
        }

    @Override
    public synchronized void remove() {
        if(prevKey == null)
            throw new IllegalStateException("No value has been fetched from database");
        tr.clear(prevKey);
    }

}
