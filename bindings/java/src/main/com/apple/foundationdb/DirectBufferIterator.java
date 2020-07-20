/*
 * DirectBufferIterator.java
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

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Buffer structure is something like this...
 *
 * beginKey[length, offset, orEqualBegin]
 * endKey[length, offset, orEqualEnd]
 * meta[rowLimit(int), targetBytes(int), streamingMode(int), iteration(int), snapshot(boolean), reverse(boolean)]
 * lengthBeginKey, boolean, lengthEndKey,
 * [beginKey], [endKey]
 * count of keyvalues is n
 * int = n -> 4 bytes
 * int[2*n] -> 2*N*4 bytes
 * key1,value1,key2,value2,key3,value3
 *
 *
 *
 * JNIEnv *jenv, jobject, jlong tPtr, jbyteArray keyBeginBytes, jboolean orEqualBegin, jint offsetBegin,
 * 		jbyteArray keyEndBytes, jboolean orEqualEnd, jint offsetEnd, jint rowLimit, jint targetBytes,
 * 		jint streamingMode, jint iteration, jboolean snapshot, jboolean reverse
 *
 */
class DirectBufferIterator implements Iterator<KeyValue> {
    private ByteBuffer byteBuffer;
    private boolean isNew = true;
    private KeyValue currentKeyValue;
    private int current;

    public DirectBufferIterator(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public KeyValue next() {
        return null;
    }

    public int count() {
        return byteBuffer.getInt(0);
    }

    public void reset() {
        isNew = true;
    }

    public byte[] lastKey() {
        return null;
    }

    public boolean isNew() {
        return isNew;
    }

    public ByteBuffer buffer() {
        return byteBuffer;
    }

}
