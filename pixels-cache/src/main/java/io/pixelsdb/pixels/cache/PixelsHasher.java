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

/**
 * Created at: 2024/1/20
 *
 * @author alph00
 */
class PixelsHasher {
    public static int getBucketNum() {
        return bucketNum;
    }

    public static void setBucketNum(int bucketNum) {
        PixelsHasher.bucketNum = bucketNum;
    }

    private static int bucketNum = 0;

    private static

    public static int getHash(PixelsCacheKey key) {
        String keyString = key.toString();
        byte[] bytes = keyString.getBytes();
        int var1 = 1;
        for (int var3 = 0; var3 < bytes.length; ++var3) {
            var1 = 31 * var1 + bytes[var3];
        }
        return var1 % bucketNum;
    }
}