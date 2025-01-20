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
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * pixels cache reader.
 * It is not thread safe?
 *
 * @author alph00
 */
public class PixelsCacheReader implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(PixelsCacheReader.class);
    private final PixelsLocator locator;
    private final List<PixelsZoneReader> zones;
    private final PixelsBucketToZoneMap bucketToZoneMap;

    private PixelsCacheReader(PixelsLocator builderLocator, List<PixelsZoneReader> builderZones, PixelsBucketToZoneMap bucketToZoneMap) {
        this.zones = builderZones;
        this.locator = builderLocator;
        this.bucketToZoneMap = bucketToZoneMap;
    }

    public static class Builder {
        private List<MemoryMappedFile> zoneFiles;
        private List<MemoryMappedFile> indexFiles;
        private MemoryMappedFile globalIndexFile;
        int zoneNum = 0;

        private Builder() {
        }

        public PixelsCacheReader.Builder setZoneFile(List<MemoryMappedFile> zoneFiles) {
            this.zoneFiles = zoneFiles;
            zoneNum = zoneFiles.size();
            return this;
        }

        public PixelsCacheReader.Builder setIndexFile(List<MemoryMappedFile> indexFiles) {
            this.indexFiles = indexFiles;
            return this;
        }

        public PixelsCacheReader.Builder setGlobalIndexFile(MemoryMappedFile globalIndexFile) {
            this.globalIndexFile = globalIndexFile;
            return this;
        }

        public PixelsCacheReader build() {
            List<PixelsZoneReader> builderZones = new ArrayList<>();
            for (int i = 0; i < zoneNum; i++) {
                builderZones.add(PixelsZoneReader.newBuilder().setZoneFile(zoneFiles.get(i)).setIndexFile(indexFiles.get(i)).build());
            }
            PixelsLocator builderLocator = new PixelsLocator(zoneNum);
            PixelsBucketToZoneMap bucketToZoneMap = new PixelsBucketToZoneMap(globalIndexFile, zoneNum);
            return new PixelsCacheReader(builderLocator, builderZones, bucketToZoneMap);
        }
    }

    public static PixelsCacheReader.Builder newBuilder() {
        return new PixelsCacheReader.Builder();
    }

    public ByteBuffer get(long blockId, short rowGroupId, short columnId) {
        long bucketId = locator.getLocation(new PixelsCacheKey(blockId, rowGroupId, columnId));
        int zoneId = bucketToZoneMap.getBucketToZone(bucketId);
        return this.zones.get(zoneId).get(blockId, rowGroupId, columnId, true);
    }

    public void close() {
        try {
//            logger.info("cache reader unmaps cache/index file");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
