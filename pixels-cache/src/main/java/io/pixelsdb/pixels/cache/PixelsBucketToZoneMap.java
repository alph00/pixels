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

import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;

import java.util.List;

/**
 * Created at: 2024/2/17
 *
 * @author alph00
 */
public class PixelsBucketToZoneMap {
    MemoryMappedFile indexFile;
    long zoneNum;
    long startOfRwAndCounts;
    final long startOfMap = 14;
    final long sizeOfRwAndCount = 4;
    final long sizeOfBucketToZone = 4;
    final long sizeOfSlot = sizeOfRwAndCount + sizeOfBucketToZone;
    final long stepOfSlot = 3;

    PixelsBucketToZoneMap(MemoryMappedFile indexFile,long zoneNum) {
        this.indexFile = indexFile;
        this.zoneNum = zoneNum;
        this.startOfRwAndCounts = this.startOfMap + this.sizeOfBucketToZone;
    }

    public void updateBucketZoneMap(long bucketId, int zoneId) {
        indexFile.setIntVolatile(this.startOfMap + bucketId << this.stepOfSlot, zoneId);
    }

    public boolean initialize() {
        if (indexFile.getSize() < this.startOfMap + this.zoneNum << this.stepOfSlot) {
            return false;
        }
        for (int i = 0; i < this.zoneNum; i++) {
            indexFile.setIntVolatile(this.startOfMap + i << this.stepOfSlot, i);
            indexFile.setIntVolatile(this.startOfRwAndCounts + i << this.stepOfSlot, 0);
        }
        return true;
    }

    public int getBucketToZone(long bucketId) {
        return indexFile.getIntVolatile(this.startOfMap + bucketId << this.stepOfSlot);
    }

    public void beginWrite(long bucketId) throws InterruptedException {
        long pos = this.startOfRwAndCounts + bucketId << this.stepOfSlot;
        // Set the rw flag.
        indexFile.setByteVolatile(pos, (byte) 1);
        final int sleepMs = 10;
        int waitMs = 0;
        while ((indexFile.getIntVolatile(pos) & PixelsZoneUtil.READER_COUNT_MASK) > 0) // polling to see if something is finished
        {
            /**
             * Wait for the existing readers to finish.
             * As rw flag has been set, there will be no new readers,
             * the existing readers should finished zone reading in
             * 10s (10000ms). If the reader can not finish zone reading
             * in 10s, it is considered as failed.
             */
            Thread.sleep(sleepMs);
            waitMs += sleepMs;
            if (waitMs > PixelsZoneUtil.ZONE_READ_LEASE_MS) {
                // clear reader count to continue writing.
                indexFile.setIntVolatile(pos, PixelsZoneUtil.ZERO_READER_COUNT_WITH_RW_FLAG);
                break;
            }
        }
    }

    public void endWrite(long bucketId) {
        long pos = this.startOfRwAndCounts + bucketId << this.stepOfSlot;
        indexFile.setByteVolatile(pos, (byte) 0);
    }

    public long beginRead(long bucketId) throws InterruptedException {
        long pos = this.startOfRwAndCounts + bucketId << this.stepOfSlot;
        int v = indexFile.getIntVolatile(pos);
        int readerCount = (v & PixelsZoneUtil.READER_COUNT_MASK) >> PixelsZoneUtil.READER_COUNT_RIGHT_SHIFT_BITS;
        if (readerCount >= PixelsZoneUtil.MAX_READER_COUNT) {
            throw new InterruptedException("Reaches the max concurrent read count.");
        }
        while ((v & PixelsZoneUtil.RW_MASK) > 0 ||
                // cas ensures that reading rw flag and increasing reader count is atomic.
                indexFile.compareAndSwapInt(pos, v, v + PixelsZoneUtil.READER_COUNT_INC) == false) {
            // We failed to get read lock or increase reader count.
            if ((v & PixelsZoneUtil.RW_MASK) > 0) {
                // if there is an existing writer, sleep for 10ms.
                Thread.sleep(10);
            }
            v = indexFile.getIntVolatile(pos);
            readerCount = (v & PixelsZoneUtil.READER_COUNT_MASK) >> PixelsZoneUtil.READER_COUNT_RIGHT_SHIFT_BITS;
            if (readerCount >= PixelsZoneUtil.MAX_READER_COUNT) {
                throw new InterruptedException("Reaches the max concurrent read count.");
            }
        }
        // return lease
        return System.currentTimeMillis();
    }

    public boolean endRead(long bucketId, long lease) {
        long pos = this.startOfRwAndCounts + bucketId << this.stepOfSlot;
        if (System.currentTimeMillis() - lease >= PixelsZoneUtil.ZONE_READ_LEASE_MS) {
            return false;
        }
        int v = indexFile.getIntVolatile(pos);
        // if reader count is already <= 0, nothing will be done.
        while ((v & PixelsZoneUtil.READER_COUNT_MASK) > 0) {
            if (indexFile.compareAndSwapInt(pos, v, v - PixelsZoneUtil.READER_COUNT_INC)) {
                // if v is not changed and the reader count is successfully decreased, break.
                break;
            }
            v = indexFile.getIntVolatile(pos);
        }
        return true;
    }
}