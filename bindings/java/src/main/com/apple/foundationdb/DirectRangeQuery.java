/*
 * DirectRangeQuery.java
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

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DirectRangeQuery implements AsyncIterable<KeyValue>, Iterable<KeyValue> {
    private final FDBTransaction tr;
    private KeySelector begin;
    private KeySelector end;
    private final boolean snapshot;
    private final int rowLimit;
    private final boolean reverse;
    private final StreamingMode streamingMode;

    DirectRangeQuery(FDBTransaction transaction, boolean isSnapshot,
               KeySelector begin, KeySelector end, int rowLimit,
               boolean reverse, StreamingMode streamingMode) {
        this.tr = transaction;
        this.begin = begin;
        this.end = end;
        this.snapshot = isSnapshot;
        this.rowLimit = rowLimit;
        this.reverse = reverse;
        this.streamingMode = streamingMode;
    }

    @Override
    public AsyncIterator<KeyValue> iterator() {
        return new DirectRangeIterator(tr, snapshot, begin, end, rowLimit, reverse, streamingMode);
    }

    @Override
    public CompletableFuture<List<KeyValue>> asList() {
        StreamingMode mode = this.streamingMode;
        if(mode == StreamingMode.ITERATOR)
            mode = (this.rowLimit == 0) ? StreamingMode.WANT_ALL : StreamingMode.EXACT;

        // if the streaming mode is EXACT, try and grab things as one chunk

        if(mode == StreamingMode.EXACT) {
            FutureResults range = tr.getRange_internal(
                    this.begin, this.end, this.rowLimit, 0, StreamingMode.EXACT.code(),
                    1, this.snapshot, this.reverse);
            return range.thenApply(result -> result.get().values)
                    .whenComplete((result, e) -> range.close());
        }

        // If the streaming mode is not EXACT, simply collect the results of an iteration into a list
        return AsyncUtil.collect(
                new RangeQuery(tr, snapshot, begin, end, rowLimit, reverse, mode), tr.getExecutor());
    }

}