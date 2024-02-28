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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created at: 2024/1/23
 *
 * @author alph00
 */
public class PixelsBucketTypeInfo {
    private final static Logger logger = LogManager.getLogger(PixelsBucketTypeInfo.class);
    private int allBucketNum;
    private int swapBucketNum;
    private int lazyBucketNum;
    private int eagerBucketNum;

    private final List<Integer> lazyBucketIds;
    private final List<Integer> swapBucketIds;
    private final List<Integer> eagerBucketIds;

    private PixelsBucketTypeInfo(Builder builder) {
        this.allBucketNum = builder.allBucketNum;
        this.swapBucketNum = builder.swapBucketNum;
        this.lazyBucketNum = builder.lazyBucketNum;
        this.eagerBucketNum = builder.eagerBucketNum;
        this.lazyBucketIds = builder.lazyBucketIds;
        this.swapBucketIds = builder.swapBucketIds;
        this.eagerBucketIds = builder.eagerBucketIds;
    }

    public static class Builder {
        private int allBucketNum;
        private int swapBucketNum = 1;
        private int eagerBucketNum = 0;
        private int lazyBucketNum = allBucketNum - swapBucketNum - eagerBucketNum;

        private List<Integer> lazyBucketIds = new ArrayList<>();
        private List<Integer> swapBucketIds = new ArrayList<>();
        private List<Integer> eagerBucketIds = new ArrayList<>();

        public Builder(int allBucketNum) {
            this.allBucketNum = allBucketNum;
//            if (allZoneNum < 1) {
//                logger.error("allZoneNum must be greater than 0");
//            } else if(allZoneNum == 1){
//                swapZoneNum = 0;
//            } else {
//                this.swapZoneIds.add(allZoneNum - 1);
//            }
//            this.lazyZoneIds.addAll(IntStream.range(0, lazyZoneNum).boxed().collect(Collectors.toList()));
        }

        public Builder setAllBucketNum(int allBucketNum) {
            this.allBucketNum = allBucketNum;
            return this;
        }

        public Builder setLazyBucketNum(int lazyBucketNum) {
            this.lazyBucketNum = lazyBucketNum;
            return this;
        }

        public Builder setSwapBucketNum(int swapBucketNum) {
            this.swapBucketNum = swapBucketNum;
            return this;
        }

        public Builder setEagerBucketNum(int eagerBucketNum) {
            this.eagerBucketNum = eagerBucketNum;
            return this;
        }

        public Builder setLazyBucketIds(List<Integer> lazyBucketIds) {
            this.lazyBucketIds = lazyBucketIds;
            return this;
        }

        public Builder setSwapBucketIds(List<Integer> swapBucketIds) {
            this.swapBucketIds = swapBucketIds;
            return this;
        }

        public Builder setEagerBucketIds(List<Integer> eagerBucketIds) {
            this.eagerBucketIds = eagerBucketIds;
            return this;
        }

        public PixelsBucketTypeInfo build() {
            return new PixelsBucketTypeInfo(this);
        }
    }

    public static Builder newBuilder(int lazyBucketNum) {
        return new Builder(lazyBucketNum);
    }

    public int getAllBucketNum() {
        return allBucketNum;
    }

    public void setAllBucketNum(int allBucketNum) {
        this.allBucketNum = allBucketNum;
    }

    public int getSwapBucketNum() {
        return swapBucketNum;
    }

    public void setSwapBucketNum(int swapBucketNum) {
        this.swapBucketNum = swapBucketNum;
    }

    public int getLazyBucketNum() {
        return lazyBucketNum;
    }

    public void setLazyBucketNum(int lazyBucketNum) {
        this.lazyBucketNum = lazyBucketNum;
    }

    public int getEagerBucketNum() {
        return eagerBucketNum;
    }

    public void setEagerBucketNum(int eagerBucketNum) {
        this.eagerBucketNum = eagerBucketNum;
    }

    public void incrementLazyBucketNum() {
        this.lazyBucketNum++;
    }

    public void incrementSwapBucketNum() {
        this.swapBucketNum++;
    }

    public void incrementEagerBucketNum() {
        this.eagerBucketNum++;
    }

    public List<Integer> getLazyBucketIds() {
        return lazyBucketIds;
    }

    public List<Integer> getSwapBucketIds() {
        return swapBucketIds;
    }

    public List<Integer> getEagerBucketIds() {
        return eagerBucketIds;
    }
}