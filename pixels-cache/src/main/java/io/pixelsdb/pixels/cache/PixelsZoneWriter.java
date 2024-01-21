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

/**
 * Created at: 2024/1/17
 *
 * @author alph00
 */
public class PixelsZoneWriter {
    public final MemoryMappedFile zoneFile;
    public final MemoryMappedFile indexFile;
    public PixelsRadix radix;
    public long currentIndexOffset;
    public long allocatedIndexOffset = PixelsZoneUtil.INDEX_RADIX_OFFSET;
    public long cacheOffset = PixelsZoneUtil.ZONE_DATA_OFFSET; // this is only used in the write() method.

    public PixelsZoneWriter(String builderZoneLocation, String builderIndexLocation, long builderZoneSize, long builderIndexSize) throws Exception {
        zoneFile = new MemoryMappedFile(builderZoneLocation, builderZoneSize);
        indexFile = new MemoryMappedFile(builderIndexLocation, builderIndexSize);
    }

    public void buildIndex(PixelsCacheConfig cacheConfig) throws Exception {
        radix = new PixelsRadix();
        PixelsZoneUtil.initialize(indexFile, zoneFile);
    }

    public void loadIndex() throws Exception {
        radix = PixelsZoneUtil.loadRadixIndex(indexFile);
    }

    public MemoryMappedFile getIndexFile() {
        return indexFile;
    }

    public MemoryMappedFile getZoneFile() {
        return zoneFile;
    }

    public boolean isZoneEmpty() {
        return PixelsZoneUtil.getStatus(this.zoneFile) == PixelsZoneUtil.ZoneStatus.EMPTY.getId() &&
                PixelsZoneUtil.getSize(this.zoneFile) == 0;
    }

    public void close() throws Exception {
        indexFile.unmap();
        zoneFile.unmap();
    }
}
