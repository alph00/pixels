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

/**
 * pixels cache getter.
 * It is not thread safe?
 *
 * @author alph00
 */
public class PixelsCacheGetter implements AutoCloseable
{
    private static final Logger logger = LogManager.getLogger(PixelsCacheGetter.class);
    public void close()
    {
        try
        {
//            logger.info("cache reader unmaps cache/index file");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
