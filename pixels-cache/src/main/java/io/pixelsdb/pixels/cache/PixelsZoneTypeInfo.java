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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created at: 2024/1/23
 *
 * @author alph00
 */
public class PixelsZoneTypeInfo {
    private final static Logger logger = LogManager.getLogger(PixelsZoneTypeInfo.class);
    private int allZoneNum;
    private int swapZoneNum;
    private int lazyZoneNum;
    private int eagerZoneNum;

    private final List<Integer> lazyZoneIds;
    private final List<Integer> swapZoneIds;
    private final List<Integer> eagerZoneIds;

    private PixelsZoneTypeInfo(Builder builder) {
        this.allZoneNum = builder.allZoneNum;
        this.swapZoneNum = builder.swapZoneNum;
        this.lazyZoneNum = builder.lazyZoneNum;
        this.eagerZoneNum = builder.eagerZoneNum;
        this.lazyZoneIds = builder.lazyZoneIds;
        this.swapZoneIds = builder.swapZoneIds;
        this.eagerZoneIds = builder.eagerZoneIds;
    }

    public static class Builder {
        private int allZoneNum;
        private int swapZoneNum = 1;
        private int eagerZoneNum = 0;
        private int lazyZoneNum = allZoneNum - swapZoneNum - eagerZoneNum;

        private List<Integer> lazyZoneIds = new ArrayList<>();
        private List<Integer> swapZoneIds = new ArrayList<>();
        private List<Integer> eagerZoneIds = new ArrayList<>();

        public Builder(int allZoneNum) {
            this.allZoneNum = allZoneNum;
//            if (allZoneNum < 1) {
//                logger.error("allZoneNum must be greater than 0");
//            } else if(allZoneNum == 1){
//                swapZoneNum = 0;
//            } else {
//                this.swapZoneIds.add(allZoneNum - 1);
//            }
//            this.lazyZoneIds.addAll(IntStream.range(0, lazyZoneNum).boxed().collect(Collectors.toList()));
        }

        public Builder setAllZoneNum(int allZoneNum) {
            this.allZoneNum = allZoneNum;
            return this;
        }

        public Builder setLazyZoneNum(int lazyZoneNum) {
            this.lazyZoneNum = lazyZoneNum;
            return this;
        }

        public Builder setSwapZoneNum(int swapZoneNum) {
            this.swapZoneNum = swapZoneNum;
            return this;
        }

        public Builder setEagerZoneNum(int eagerZoneNum) {
            this.eagerZoneNum = eagerZoneNum;
            return this;
        }

        public Builder setLazyZoneIds(List<Integer> lazyZoneIds) {
            this.lazyZoneIds = lazyZoneIds;
            return this;
        }

        public Builder setSwapZoneIds(List<Integer> swapZoneIds) {
            this.swapZoneIds = swapZoneIds;
            return this;
        }

        public Builder setEagerZoneIds(List<Integer> eagerZoneIds) {
            this.eagerZoneIds = eagerZoneIds;
            return this;
        }

        public PixelsZoneTypeInfo build() {
            return new PixelsZoneTypeInfo(this);
        }
    }

    public static Builder newBuilder(int lazyZoneNum) {
        return new Builder(lazyZoneNum);
    }

    public int getAllZoneNum() {
        return allZoneNum;
    }

    public void setAllZoneNum(int allZoneNum) {
        this.allZoneNum = allZoneNum;
    }

    public int getSwapZoneNum() {
        return swapZoneNum;
    }

    public void setSwapZoneNum(int swapZoneNum) {
        this.swapZoneNum = swapZoneNum;
    }

    public int getLazyZoneNum() {
        return lazyZoneNum;
    }

    public void setLazyZoneNum(int lazyZoneNum) {
        this.lazyZoneNum = lazyZoneNum;
    }

    public int getEagerZoneNum() {
        return eagerZoneNum;
    }

    public void setEagerZoneNum(int eagerZoneNum) {
        this.eagerZoneNum = eagerZoneNum;
    }

    public void incrementLazyZoneNum() {
        this.lazyZoneNum++;
    }

    public void incrementSwapZoneNum() {
        this.swapZoneNum++;
    }

    public void incrementEagerZoneNum() {
        this.eagerZoneNum++;
    }

    public List<Integer> getLazyZoneIds() {
        return lazyZoneIds;
    }

    public List<Integer> getSwapZoneIds() {
        return swapZoneIds;
    }

    public List<Integer> getEagerZoneIds() {
        return eagerZoneIds;
    }
}