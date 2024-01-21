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

import java.util.HashMap;

/**
 * Created at: 2024/1/20
 *
 * @author alph00
 */
class PixelsHashMap {
    HashMap<PixelsCacheKey, Integer> hashMap;

    PixelsHashMap() {
        hashMap = new HashMap<>();
    }

    //TODO: load hashmap index
    public void loadHashMapIndex(MemoryMappedFile indexFile) {
        hashMap = new HashMap<>();
    }

    public void put(PixelsCacheKey key, Integer value) {
        hashMap.put(key, value);
    }

    public void clear() {
        hashMap.clear();
    }
}