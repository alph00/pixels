/*
 * Copyright 2019 PixelsDB.
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

import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Compact;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.core.PixelsProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pixels cache writer
 *
 * @author guodong
 * @author hank
 */
public class PixelsCacheWriter
{
    private final static Logger logger = LogManager.getLogger(PixelsCacheWriter.class);

    private final List<PixelsZoneWriter> zones;
    private final PixelsZoneTypeInfo zoneTypeInfo;
    private final MemoryMappedFile globalIndexFile;
    private final Storage storage;
    private final EtcdUtil etcdUtil;
    /**
     * The host name of the node where this cache writer is running.
     */
    private final String host;
    private Set<String> cachedColumnChunks = new HashSet<>();

    private PixelsCacheWriter(List<PixelsZoneWriter> zones,
                              PixelsZoneTypeInfo zoneTypeInfo,
                              MemoryMappedFile globalIndexFile,
                              Storage storage,
                              Set<String> cachedColumnChunks,
                              EtcdUtil etcdUtil,
                              String host)
    {
        this.zones= zones;
        this.storage = storage;
        this.etcdUtil = etcdUtil;
        this.host = host;
        this.globalIndexFile = globalIndexFile;
        this.zoneTypeInfo = zoneTypeInfo;
        if (cachedColumnChunks != null && cachedColumnChunks.isEmpty() == false)
        {
            cachedColumnChunks.addAll(cachedColumnChunks);
        }
    }

    public static class Builder
    {
        private String builderZoneLocation = "";
        private long builderZoneSize;
        private String builderIndexLocation = "";
        private long builderIndexSize;
        private boolean builderOverwrite = true;
        private String builderHostName = null;
        private PixelsCacheConfig cacheConfig = null;
        private int zoneNum = 1;
        private PixelsZoneTypeInfo zoneTypeInfo = null;

        private Builder()
        {
        }

        public Builder setZoneLocation(String zoneLocation)
        {
            checkArgument(zoneLocation != null && !zoneLocation.isEmpty(),
                    "cache location should bot be empty");
            this.builderZoneLocation = zoneLocation;

            return this;
        }

        public Builder setZoneSize(long zoneSize)
        {
            checkArgument(zoneSize > 0, "cache size should be positive");
            this.builderZoneSize = zoneSize;
            return this;
        }

        public Builder setIndexLocation(String indexLocation)
        {
            checkArgument(indexLocation != null && !indexLocation.isEmpty(),
                    "index location should not be empty");
            this.builderIndexLocation = indexLocation;

            return this;
        }

        public Builder setIndexSize(long size)
        {
            checkArgument(size > 0, "index size should be positive");
            this.builderIndexSize = size;

            return this;
        }

        public Builder setOverwrite(boolean overwrite)
        {
            this.builderOverwrite = overwrite;
            return this;
        }

        public Builder setHostName(String hostName)
        {
            checkArgument(hostName != null, "hostname should not be null");
            this.builderHostName = hostName;
            return this;
        }

        public Builder setCacheConfig(PixelsCacheConfig cacheConfig)
        {
            checkArgument(cacheConfig != null, "cache config should not be null");
            this.cacheConfig = cacheConfig;
            return this;
        }

        public Builder setZoneNum(int zoneNum)
        {
            checkArgument(zoneNum > 0, "zone number should be positive");
            this.zoneNum=zoneNum;
            return this;
        }

        public PixelsCacheWriter build()
                throws Exception
        {
            zoneTypeInfo = PixelsZoneTypeInfo.newBuilder(zoneNum).build();

            List<PixelsZoneWriter> zones = new ArrayList<>(zoneNum);
            for(int i=0; i<zoneNum; i++)
            {
                zones.add(new PixelsZoneWriter(builderZoneLocation+"."+String.valueOf(i),builderIndexLocation+"."+String.valueOf(i),builderZoneSize,builderIndexSize));
            }

            MemoryMappedFile globalIndexFile = new MemoryMappedFile(builderIndexLocation, builderIndexSize);

            Set<String> cachedColumnChunks = new HashSet<>();

            // check if cache and index exists.
            // if overwrite is not true, and cache and index file already exists, reconstruct radix from existing index.
            if (!builderOverwrite && PixelsZoneUtil.checkMagic(globalIndexFile) && PixelsZoneUtil.checkMagic(globalIndexFile))
            {
                // cache exists in local cache file and index, reload the index.
                for(int i=0; i<zoneNum; i++)
                {
                    zones.get(i).loadIndex();
                    switch (zones.get(i).getZoneType()){
                        case LAZY:
                            zoneTypeInfo.incrementLazyZoneNum();
                            zoneTypeInfo.getLazyZoneIds().add(i);
                            break;
                        case SWAP:
                            zoneTypeInfo.incrementSwapZoneNum();
                            zoneTypeInfo.getSwapZoneIds().add(i);
                            break;
                        case EAGER:
                            zoneTypeInfo.incrementEagerZoneNum();
                            zoneTypeInfo.getEagerZoneIds().add(i);
                            break;
                        default:
                            logger.warn("zone type error");
                    }
                }
                // build cachedColumnChunks for PixelsCacheWriter.
                int cachedVersion = PixelsZoneUtil.getIndexVersion(globalIndexFile);
                MetadataService metadataService = new MetadataService(
                        cacheConfig.getMetaHost(), cacheConfig.getMetaPort());
                Layout cachedLayout = metadataService.getLayout(
                        cacheConfig.getSchema(), cacheConfig.getTable(), cachedVersion);
                Compact compact = cachedLayout.getCompact();
                int cacheBorder = compact.getCacheBorder();
                cachedColumnChunks.addAll(compact.getColumnChunkOrder().subList(0, cacheBorder));
                metadataService.shutdown();
            }
            else
            {
                PixelsZoneUtil.initializeIndex(globalIndexFile);
                zoneTypeInfo.setLazyZoneNum(zoneNum - 1);
                zoneTypeInfo.getLazyZoneIds().addAll(new ArrayList<Integer>(){{for(int i=0;i<zoneNum-1;i++)add(i);}});
                for (int i = 0; i < zoneNum - 1; i++)
                {
                    zones.get(i).buildLazy(cacheConfig);
                }
                // initialize swap zone
                zoneTypeInfo.setSwapZoneNum(1);
                zoneTypeInfo.getSwapZoneIds().add(zoneNum - 1);
                zones.get(zoneNum - 1).buildSwap(cacheConfig);
            }

            // initialize the hashFunction with the number of zones.
            PixelsHasher.setBucketNum(zoneTypeInfo.getLazyZoneNum());

            EtcdUtil etcdUtil = EtcdUtil.Instance();

            Storage storage = StorageFactory.Instance().getStorage(cacheConfig.getStorageScheme());

            return new PixelsCacheWriter(zones, zoneTypeInfo,globalIndexFile,storage,
                    cachedColumnChunks, etcdUtil, builderHostName);
        }
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public MemoryMappedFile getIndexFile(int zoneId)
    {
        return zones.get(zoneId).getIndexFile();
    }

/*
    /**
     * DO NOT USE THIS METHOD. Only for unit test.
     * @return
     * /
    public PixelsRadix getRadix()
    {
        return this.radix;
    }
*/

    /**
     * <p>
     * This function is only used to bulk load all the cache content at one time.
     * Readers will be blocked until this function is finished.
     * </p>
     * Return code:
     * -1: update failed.
     * 0: no updates are needed or update successfully.
     * 2: update size exceeds the limit.
     */
    public int updateAll(int version, Layout layout)
    {
        try
        {
            // get the caching file list
            String key = Constants.CACHE_LOCATION_LITERAL + version + "_" + host;
            KeyValue keyValue = etcdUtil.getKeyValue(key);
            if (keyValue == null)
            {
                logger.debug("Found no allocated files. No updates are needed. " + key);
                return 0;
            }
            String fileStr = keyValue.getValue().toString(StandardCharsets.UTF_8);
            String[] files = fileStr.split(";");
            return internalUpdateAll(version, layout, files);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * return true if all the zones are empty
     */
    public boolean isCacheEmpty ()
    {
        /**
         * There are no concurrent updates on the cache,
         * thus we don't have to synchronize the access to cachedColumnChunks.
         */
        for(PixelsZoneWriter zone:zones)
        {
            if(zone.getZoneType() != PixelsZoneUtil.ZoneType.SWAP && !zone.isZoneEmpty())
            {
                return false;
            }
        }
        return true;
        // return cachedColumnChunks == null || cachedColumnChunks.isEmpty();
    }

    public boolean isExistZoneEmpty ()
    {
        for(PixelsZoneWriter zone:zones)
        {
            if(zone.getZoneType() != PixelsZoneUtil.ZoneType.SWAP && zone.isZoneEmpty())
            {
                return true;
            }
        }
        return false;
    }

    /**
     * <p>
     * This function is used to update the cache content incrementally,
     * using the three-phase update protocol. Which means readers are only
     * blocked during the first (compaction) and the third (accomplishing)
     * phase. While in the second (loading) also most expensive phase, readers
     * are not blocked.
     * </p>
     * Return code:
     * -1: update failed.
     * 0: no updates are needed or update successfully.
     * 2: update size exceeds the limit.
     */
    public int updateIncremental (int version, Layout layout)
    {
        try
        {
            // get the caching file list
            String key = Constants.CACHE_LOCATION_LITERAL + version + "_" + host;
            KeyValue keyValue = etcdUtil.getKeyValue(key);
            if (keyValue == null)
            {
                logger.debug("Found no allocated files. No updates are needed. " + key);
                return 0;
            }
            String fileStr = keyValue.getValue().toString(StandardCharsets.UTF_8);
            String[] files = fileStr.split(";");
            return internalUpdateIncremental(version, layout, files);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return -1;
        }
    }

/*
    /**
     * Currently, this is an interface for unit tests.
     * This method only updates index content and cache content (without touching headers)
     * /
    public void write(PixelsCacheKey key, byte[] value)
    {
        PixelsCacheIdx cacheIdx = new PixelsCacheIdx(cacheOffset, value.length);
        cacheFile.setBytes(cacheOffset, value);
        cacheOffset += value.length;
        radix.put(key, cacheIdx);
    }
*/

    /**
     * Currently, this is an interface for unit tests.
     */
    public void flush()
    {
        for (PixelsZoneWriter zone:zones)
        {
            zone.flushIndex();
        }
    }

    private int internalUpdateAll(int version, Layout layout, String[] files)
            throws IOException
    {
        int status = 0;
        // get the new caching layout
        Compact compact = layout.getCompact();
        int cacheBorder = compact.getCacheBorder();
        List<String> cacheColumnChunkOrders = compact.getColumnChunkOrder().subList(0, cacheBorder);
        // set rwFlag as write
        logger.debug("Set index rwFlag as write");
        try
        {
            /**
             * Before updating the cache content, in beginIndexWrite:
             * 1. Set rwFlag to block subsequent readers.
             * 2. Wait for the existing readers to finish, i.e.
             *    wait for the readCount to be cleared (become zero).
             */
            PixelsZoneUtil.beginIndexWrite(globalIndexFile);
        } catch (InterruptedException e)
        {
            status = -1;
            logger.error("Failed to get write permission on index.", e);
            return status;
        }

        // update cache content
        if (cachedColumnChunks == null || cachedColumnChunks.isEmpty())
        {
            cachedColumnChunks = new HashSet<>(cacheColumnChunkOrders.size());
        }
        else
        {
            cachedColumnChunks.clear();
        }
        long currCacheOffset = PixelsZoneUtil.ZONE_DATA_OFFSET;
        boolean enableAbsoluteBalancer = Boolean.parseBoolean(
                ConfigFactory.Instance().getProperty("enable.absolute.balancer"));
        outer_loop:
        for (String file : files)
        {
            if (enableAbsoluteBalancer && storage.hasLocality())
            {
                // TODO: this is used for experimental purpose only.
                // may be removed later.
                file = ensureLocality(file);
            }
            PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(storage, file);
            int physicalLen;
            long physicalOffset;
            // update radix and cache content
            for (int i = 0; i < cacheColumnChunkOrders.size(); i++)
            {
                String[] columnChunkIdStr = cacheColumnChunkOrders.get(i).split(":");
                short rowGroupId = Short.parseShort(columnChunkIdStr[0]);
                short columnId = Short.parseShort(columnChunkIdStr[1]);
                PixelsCacheKey key = new PixelsCacheKey(pixelsPhysicalReader.getCurrentBlockId(), rowGroupId, columnId);
                int zoneId = PixelsHasher.getHash(key);
                PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(rowGroupId);
                PixelsProto.ColumnChunkIndex chunkIndex =
                        rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(columnId);
                physicalLen = (int) chunkIndex.getChunkLength();
                physicalOffset = chunkIndex.getChunkOffset();
                if (currCacheOffset + physicalLen >= zones.get(zoneId).getZoneFile().getSize())
                {
                    logger.debug("Cache writes have exceeded cache size. Break. Current size: " + currCacheOffset);
                    status = 2;
                    break outer_loop;
                }
                else
                {
                    zones.get(zoneId).getRadix().put(new PixelsCacheKey(pixelsPhysicalReader.getCurrentBlockId(), rowGroupId, columnId),
                            new PixelsCacheIdx(currCacheOffset, physicalLen));
                    byte[] columnChunk = pixelsPhysicalReader.read(physicalOffset, physicalLen);
                    zones.get(zoneId).getZoneFile().setBytes(currCacheOffset, columnChunk);
                    logger.debug(
                            "Cache write: " + file + "-" + rowGroupId + "-" + columnId + ", offset: " + currCacheOffset + ", length: " + columnChunk.length);
                    currCacheOffset += physicalLen;
                }
            }
        }
        for (String cachedColumnChunk : cacheColumnChunkOrders)
        {
            cachedColumnChunks.add(cachedColumnChunk);
        }
        logger.debug("Cache writer ends at offset: " + currCacheOffset);
        // flush index
        for(Integer zoneId:zoneTypeInfo.getLazyZoneIds())
        {
            zones.get(zoneId).flushIndex();
        }
        // update cache version
        PixelsZoneUtil.setIndexVersion(globalIndexFile, version);
        for(Integer zoneId:zoneTypeInfo.getLazyZoneIds())
        {
            PixelsZoneUtil.setStatus(zones.get(zoneId).getZoneFile(), PixelsZoneUtil.ZoneStatus.OK.getId());
            PixelsZoneUtil.setSize(zones.get(zoneId).getZoneFile(), currCacheOffset);
        }
        // set rwFlag as readable
        PixelsZoneUtil.endIndexWrite(globalIndexFile);
        // logger.debug("Cache index ends at offset: " + currentIndexOffset);

        return status;
    }

    /**
     * This method needs further tests.
     * @param version
     * @param layout
     * @param files
     * @return
     * @throws IOException
     */
    private int internalUpdateIncremental(int version, Layout layout, String[] files)
            throws IOException
    {
        int status = 0;
        /**
         * Get the new caching layout.
         */
        Compact compact = layout.getCompact();
        int cacheBorder = compact.getCacheBorder();
        List<String> nextVersionCached = compact.getColumnChunkOrder().subList(0, cacheBorder);
        /**
         * Prepare structures for the survived and new coming cache elements.
         */
        List<String> survivedColumnChunks = new ArrayList<>();
        List<String> newCachedColumnChunks = new ArrayList<>();
        for (String columnChunk : nextVersionCached)
        {
            if (this.cachedColumnChunks.contains(columnChunk))
            {
                survivedColumnChunks.add(columnChunk);
            }
            else
            {
                newCachedColumnChunks.add(columnChunk);
            }
        }
        this.cachedColumnChunks.clear();
        PixelsRadix oldRadix = radix;
        List<PixelsCacheEntry> survivedIdxes = new ArrayList<>(survivedColumnChunks.size()*files.length);
        for (String file : files)
        {
            PixelsPhysicalReader physicalReader = new PixelsPhysicalReader(storage, file);
            // TODO: in case of block id was changed, the survived column chunks in this block can not survive in the cache update.
            // This problem only affects the efficiency, but it is better to resolve it.
            long blockId = physicalReader.getCurrentBlockId();
            for (String survivedColumnChunk : survivedColumnChunks)
            {
                String[] columnChunkIdStr = survivedColumnChunk.split(":");
                short rowGroupId = Short.parseShort(columnChunkIdStr[0]);
                short columnId = Short.parseShort(columnChunkIdStr[1]);
                PixelsCacheIdx curCacheIdx = oldRadix.get(blockId, rowGroupId, columnId);
                survivedIdxes.add(
                        new PixelsCacheEntry(new PixelsCacheKey(
                                physicalReader.getCurrentBlockId(), rowGroupId, columnId), curCacheIdx));
            }
        }
        // ascending order according to the offset in cache file.
        Collections.sort(survivedIdxes);

        /**
         * Start phase 1: compact the survived cache elements.
         */
        logger.debug("Start cache compaction...");
        long newCacheOffset = PixelsCacheUtil.CACHE_DATA_OFFSET;
        PixelsRadix newRadix = new PixelsRadix();
        // set rwFlag as write
        try
        {
            /**
             * Before updating the cache content, in beginIndexWrite:
             * 1. Set rwFlag to block subsequent readers.
             * 2. Wait for the existing readers to finish, i.e.
             *    wait for the readCount to be cleared (become zero).
             */
            PixelsCacheUtil.beginIndexWrite(indexFile);
        } catch (InterruptedException e)
        {
            status = -1;
            logger.error("Failed to get write permission on index.", e);
            return status;
        }
        for (PixelsCacheEntry survivedIdx : survivedIdxes)
        {
            cacheFile.copyMemory(survivedIdx.idx.offset, newCacheOffset, survivedIdx.idx.length);
            newRadix.put(survivedIdx.key, new PixelsCacheIdx(newCacheOffset, survivedIdx.idx.length));
            newCacheOffset += survivedIdx.idx.length;
        }
        this.radix = newRadix;
        // flush index
        flushIndex();
        // set rwFlag as readable
        PixelsCacheUtil.endIndexWrite(indexFile);
        oldRadix.removeAll();
        PixelsCacheUtil.setCacheStatus(cacheFile, PixelsCacheUtil.CacheStatus.OK.getId());
        PixelsCacheUtil.setCacheSize(cacheFile, newCacheOffset);
        // save the survived column chunks into cachedColumnChunks.
        this.cachedColumnChunks.addAll(survivedColumnChunks);
        logger.debug("Cache compaction finished, index ends at offset: " + currentIndexOffset);

        /**
         * Start phase 2: load and append new cache elements to the cache file.
         */
        logger.debug("Start cache append...");
        List<PixelsCacheEntry> newIdxes = new ArrayList<>();
        boolean enableAbsoluteBalancer = Boolean.parseBoolean(
                ConfigFactory.Instance().getProperty("enable.absolute.balancer"));
        outer_loop:
        for (String file : files)
        {
            if (enableAbsoluteBalancer && storage.hasLocality())
            {
                // TODO: this is used for experimental purpose only.
                // may be removed later.
                file = ensureLocality(file);
            }
            PixelsPhysicalReader pixelsPhysicalReader = new PixelsPhysicalReader(storage, file);
            int physicalLen;
            long physicalOffset;
            // update radix and cache content
            for (int i = 0; i < newCachedColumnChunks.size(); i++)
            {
                String[] columnChunkIdStr = newCachedColumnChunks.get(i).split(":");
                short rowGroupId = Short.parseShort(columnChunkIdStr[0]);
                short columnId = Short.parseShort(columnChunkIdStr[1]);
                PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(rowGroupId);
                PixelsProto.ColumnChunkIndex chunkIndex =
                        rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(columnId);
                physicalLen = chunkIndex.getChunkLength();
                physicalOffset = chunkIndex.getChunkOffset();
                if (newCacheOffset + physicalLen >= cacheFile.getSize())
                {
                    logger.debug("Cache writes have exceeded cache size. Break. Current size: " + newCacheOffset);
                    status = 2;
                    break outer_loop;
                }
                else
                {
                    newIdxes.add(new PixelsCacheEntry(
                            new PixelsCacheKey(pixelsPhysicalReader.getCurrentBlockId(), rowGroupId, columnId),
                            new PixelsCacheIdx(newCacheOffset, physicalLen)));
                    byte[] columnChunk = pixelsPhysicalReader.read(physicalOffset, physicalLen);
                    cacheFile.setBytes(newCacheOffset, columnChunk);
                    logger.debug(
                            "Cache write: " + file + "-" + rowGroupId + "-" + columnId + ", offset: " + newCacheOffset + ", length: " + columnChunk.length);
                    newCacheOffset += physicalLen;
                }
            }
        }
        logger.debug("Cache append finished, cache writer ends at offset: " + newCacheOffset);

        /**
         * Start phase 3: activate new cache elements.
         */
        logger.debug("Start activating new cache elements...");
        // set rwFlag as write
        try
        {
            /**
             * Before updating the cache content, in beginIndexWrite:
             * 1. Set rwFlag to block subsequent readers.
             * 2. Wait for the existing readers to finish, i.e.
             *    wait for the readCount to be cleared (become zero).
             */
            PixelsCacheUtil.beginIndexWrite(indexFile);
        } catch (InterruptedException e)
        {
            status = -1;
            logger.error("Failed to get write permission on index.", e);
            // TODO: recovery needed here.
            return status;
        }
        for (PixelsCacheEntry newIdx : newIdxes)
        {
            radix.put(newIdx.key, newIdx.idx);
        }
        // flush index
        flushIndex();
        // save the new cached column chunks into cachedColumnChunks.
        for (String newColumnChunk : newCachedColumnChunks)
        {
            this.cachedColumnChunks.add(newColumnChunk);
        }
        // update cache version
        PixelsCacheUtil.setIndexVersion(indexFile, version);
        PixelsCacheUtil.setCacheStatus(cacheFile, PixelsCacheUtil.CacheStatus.OK.getId());
        PixelsCacheUtil.setCacheSize(cacheFile, newCacheOffset);
        // set rwFlag as readable
        PixelsCacheUtil.endIndexWrite(indexFile);
        logger.debug("Cache index ends at offset: " + currentIndexOffset);

        return status;
    }

    /**
     * This method is currently used for experimental purpose.
     * @param path
     * @return
     */
    private String ensureLocality (String path)
    {
        String newPath = path.substring(0, path.indexOf(".pxl")) + "_" + host + ".pxl";
        try
        {
            String[] dataNodes = storage.getHosts(path);
            boolean isLocal = false;
            for (String dataNode : dataNodes)
            {
                if (dataNode.equals(host))
                {
                    isLocal = true;
                    break;
                }
            }
            if (isLocal == false)
            {
                // file is not local, move it to local.
                PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(storage, path);
                PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(storage, newPath,
                        2048l*1024l*1024l, (short) 1, true);
                byte[] buffer = new byte[1024*1024*32]; // 32MB buffer for copy.
                long copiedBytes = 0l, fileLength = reader.getFileLength();
                boolean success = true;
                try
                {
                    while (copiedBytes < fileLength)
                    {
                        int bytesToCopy = 0;
                        if (copiedBytes + buffer.length <= fileLength)
                        {
                            bytesToCopy = buffer.length;
                        } else
                        {
                            bytesToCopy = (int) (fileLength - copiedBytes);
                        }
                        reader.readFully(buffer, 0, bytesToCopy);
                        writer.prepare(bytesToCopy);
                        writer.append(buffer, 0, bytesToCopy);
                        copiedBytes += bytesToCopy;
                    }
                    reader.close();
                    writer.flush();
                    writer.close();
                } catch (IOException e)
                {
                    logger.error("failed to copy file", e);
                    success = false;
                }
                if (success)
                {
                    storage.delete(path, false);
                    return newPath;
                }
                else
                {
                    storage.delete(newPath, false);
                    return path;
                }

            } else
            {
                return path;
            }
        } catch (IOException e)
        {
            logger.error("failed to ensure the locality of a file/data object.", e);
        }

        return null;
    }

    public void close() throws Exception
    {
        for(PixelsZoneWriter zone:zones){
            zone.close();
        }
        globalIndexFile.unmap();
    }
}
