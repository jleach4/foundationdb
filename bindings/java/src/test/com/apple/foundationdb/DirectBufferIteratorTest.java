/*
 * DirectBufferIteratorTest.java
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.stream.IntStream;

public class DirectBufferIteratorTest {
    private static final Random RANDOM = new Random();

    @Test
    public void testDirectBufferIteration() {
        DirectBufferIterator it = new DirectBufferIterator(getByteBuffer(10));
    }

    @Test
    public void testDirectBufferLastKey() {
        DirectBufferIterator it = new DirectBufferIterator(getByteBuffer(10));
    }

    private ByteBuffer getByteBuffer(int count) {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(2048);
        byteBuffer.putInt(count);
        IntStream.range(0,count).forEachOrdered( (val) -> {
            byteBuffer.putInt(val+4);
        });
        return byteBuffer;
    }

}
