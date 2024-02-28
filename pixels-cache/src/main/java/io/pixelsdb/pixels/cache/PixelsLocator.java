/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.cache;

import java.util.TreeMap;

/**
 * Created at: 2024/1/20
 *
 * @author alph00
 */
class PixelsLocator {
    // the number of buckets
    private int bucketNum = 0;
    // the number of replicas of each bucket
    private int replicaNum = 160;
    // specific hash function
    private PixelsHasher hasher;
    // the number of hash slots, copy from Redis
    private static final int NUM_HASH_SLOTS = 16384;
    // the suffix of virtual node
    private static final String VNODE_SUFFIX = "VNODE&&";
    // the prefix of node
    private static final String NODE_PREFIX = "BUCKET#";

    private TreeMap<Long,Long> hashCycle;

    public PixelsLocator(int bucketNum) {
        this.bucketNum = bucketNum;
        init();
    }
    public PixelsLocator(int bucketNum, int replicaNum) {
        this.bucketNum = bucketNum;
        this.replicaNum = replicaNum;
        init();
    }
    public PixelsLocator(int bucketNum, int replicaNum, PixelsHasher hasher) {
        this.bucketNum = bucketNum;
        this.replicaNum = replicaNum;
        this.hasher = hasher;
        init();
    }

    private void init(){
        hasher = new PixelsMurmurHasher();
        hashCycle = new TreeMap<>();
        for (int j = 0; j < replicaNum; j++) {
            for (int i = 0; i < bucketNum; i++) {
                String vnode = VNODE_SUFFIX + j;
                hashCycle.put(hasher.getHashCode(NODE_PREFIX + i + vnode) % NUM_HASH_SLOTS, (long) i);
            }
        }
    }

    public long getLocation(PixelsCacheKey key) {
        String keyStr = key.toString();
        long hash = hasher.getHashCode(keyStr) % NUM_HASH_SLOTS;
        Long bucket = hashCycle.ceilingKey(hash);
        return bucket == null ? hashCycle.firstKey() : bucket;
    }

    // TODO: if transporting some data to the new node necessary?
    public void addBucket() {
        for (int j = 0; j < replicaNum; j++) {
                String vnode = VNODE_SUFFIX + j;
                hashCycle.put(hasher.getHashCode(NODE_PREFIX + bucketNum + vnode) % NUM_HASH_SLOTS, (long) bucketNum);
        }
        bucketNum++;
    }

    // TODO later
    public void removeBucket() {
    }
}