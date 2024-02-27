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
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pixels cache writer
 *
 * @author guodong
 * @author hank
 * @author qfs
 */
public class PixelsCacheWriter
{
    private final static Logger logger = LogManager.getLogger(PixelsCacheWriter.class);

    private final PixelsLocator locator;
    private final List<PixelsZoneWriter> zones;
    private final PixelsBucketTypeInfo bucketTypeInfo;
    private final PixelsBucketToZoneMap bucketToZoneMap;
    // private final List<Integer> bucketToZoneMap;
    private final MemoryMappedFile globalIndexFile;
    private final Storage storage;
    private final EtcdUtil etcdUtil;
    /**
     * The host name of the node where this cache writer is running.
     */
    private final String host;
    private Set<String> cachedColumnChunks = new HashSet<>();

    private PixelsCacheWriter(PixelsLocator locator,
                              List<PixelsZoneWriter> zones,
                              PixelsBucketTypeInfo bucketTypeInfo,
                              PixelsBucketToZoneMap bucketToZoneMap,
                              MemoryMappedFile globalIndexFile,
                              Storage storage,
                              Set<String> cachedColumnChunks,
                              EtcdUtil etcdUtil,
                              String host)
    {
        this.locator = locator;
        this.zones = zones;
        this.storage = storage;
        this.etcdUtil = etcdUtil;
        this.host = host;
        this.globalIndexFile = globalIndexFile;
        this.bucketTypeInfo = bucketTypeInfo;
        this.bucketToZoneMap = bucketToZoneMap;
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
        private PixelsBucketTypeInfo bucketTypeInfo = null;

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
            bucketTypeInfo = PixelsBucketTypeInfo.newBuilder(zoneNum).build();

            List<PixelsZoneWriter> zones = new ArrayList<>(zoneNum);
            for(int i=0; i<zoneNum; i++)
            {
                zones.add(new PixelsZoneWriter(builderZoneLocation+"."+String.valueOf(i),builderIndexLocation+"."+String.valueOf(i),builderZoneSize,builderIndexSize,i));
            }

            MemoryMappedFile globalIndexFile = new MemoryMappedFile(builderIndexLocation, builderIndexSize);

            Set<String> cachedColumnChunks = new HashSet<>();

            PixelsBucketToZoneMap bucketToZoneMap = new PixelsBucketToZoneMap(globalIndexFile, zoneNum);

            // check if cache and index exists.
            // if overwrite is not true, and cache and index file already exists, reconstruct radix from existing index.
            if (!builderOverwrite && PixelsZoneUtil.checkMagic(globalIndexFile) && PixelsZoneUtil.checkMagic(globalIndexFile))
            {
                // // reload bucketToZoneMap from index.
                // PixelsZoneUtil.getBucketZoneMap(globalIndexFile, bucketToZoneMap);
                // cache exists in local cache file and index, reload the index.
                for(int i=0; i<zoneNum; i++)
                {
                    int zoneId = bucketToZoneMap.getBucketToZone(i);
                    zones.get(zoneId).loadIndex();
                    switch (zones.get(zoneId).getZoneType()){
                        case LAZY:
                            bucketTypeInfo.incrementLazyBucketNum();
                            bucketTypeInfo.getLazyBucketIds().add(i);
                            break;
                        case SWAP:
                            bucketTypeInfo.incrementSwapBucketNum();
                            bucketTypeInfo.getSwapBucketIds().add(i);
                            break;
                        case EAGER:
                            bucketTypeInfo.incrementEagerBucketNum();
                            bucketTypeInfo.getEagerBucketIds().add(i);
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
                PixelsZoneUtil.initializeGlobalIndex(globalIndexFile, bucketToZoneMap);
                bucketTypeInfo.setLazyBucketNum(zoneNum - 1);
                bucketTypeInfo.getLazyBucketIds().addAll(new ArrayList<Integer>(){{for(int i=0;i<zoneNum-1;i++)add(i);}});
                for (int i = 0; i < zoneNum - 1; i++)
                {
                    zones.get(i).buildLazy(cacheConfig);
                }
                // initialize swap zone
                bucketTypeInfo.setSwapBucketNum(1);
                bucketTypeInfo.getSwapBucketIds().add(zoneNum - 1);
                zones.get(zoneNum - 1).buildSwap(cacheConfig);
            }

            // initialize the hashFunction with the number of zones.
            PixelsLocator locator = new PixelsLocator(zoneNum);

            EtcdUtil etcdUtil = EtcdUtil.Instance();

            Storage storage = StorageFactory.Instance().getStorage(cacheConfig.getStorageScheme());

            return new PixelsCacheWriter(locator, zones, bucketTypeInfo, bucketToZoneMap, globalIndexFile, storage,
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
        catch (IOException | InterruptedException e)
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


        // update cache content
        if (cachedColumnChunks == null || cachedColumnChunks.isEmpty())
        {
            cachedColumnChunks = new HashSet<>(cacheColumnChunkOrders.size());
        }
        else
        {
            cachedColumnChunks.clear();
        }
        for(int j=0; j<bucketTypeInfo.getLazyBucketNum(); ++j)
        {
            // get origin lazy zone
            int bucketId = bucketTypeInfo.getLazyBucketIds().get(j);
            // TODO: if lock is needed here?
            int zoneId = bucketToZoneMap.getBucketToZone(bucketId);
            PixelsZoneWriter zone = zones.get(zoneId);
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
                PixelsZoneUtil.beginIndexWrite(zone.getIndexFile());
            } catch (InterruptedException e)
            {
                status = -1;
                logger.error("Failed to get write permission on index.", e);
                return status;
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
                    if(locator.getLocation(key) != bucketId){
                        continue;
                    }

                    PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(rowGroupId);
                    PixelsProto.ColumnChunkIndex chunkIndex =
                            rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(columnId);
                    physicalLen = (int) chunkIndex.getChunkLength();
                    physicalOffset = chunkIndex.getChunkOffset();
                    if (currCacheOffset + physicalLen >= zone.getZoneFile().getSize())
                    {
                        logger.debug("Cache writes have exceeded bucket size. Break. Current size: " + currCacheOffset + "current bucketId: " + bucketId);
                        status = 2;
                        break outer_loop;
                    }
                    else
                    {
                        zone.getRadix().put(new PixelsCacheKey(pixelsPhysicalReader.getCurrentBlockId(), rowGroupId, columnId),
                                new PixelsCacheIdx(currCacheOffset, physicalLen));
                        byte[] columnChunk = pixelsPhysicalReader.read(physicalOffset, physicalLen);
                        zone.getZoneFile().setBytes(currCacheOffset, columnChunk);
                        logger.debug(
                                "Cache write: " + file + "-" + rowGroupId + "-" + columnId + ", offset: " + currCacheOffset + ", length: " + columnChunk.length);
                        currCacheOffset += physicalLen;
                    }
                }
            }

            // flush index
            zone.flushIndex();
            // update cache version
            PixelsZoneUtil.setIndexVersion(zone.getIndexFile(), version);
            PixelsZoneUtil.setStatus(zone.getZoneFile(), PixelsZoneUtil.ZoneStatus.OK.getId());
            PixelsZoneUtil.setSize(zone.getZoneFile(), currCacheOffset);
            // set rwFlag as readable
            PixelsZoneUtil.endIndexWrite(zone.getIndexFile());
            // logger.debug("Cache index ends at offset: " + currentIndexOffset);
            logger.debug("Writer ends at offset: " + currCacheOffset);
        }

        for (String cachedColumnChunk : cacheColumnChunkOrders)
        {
            cachedColumnChunks.add(cachedColumnChunk);
        }
        // update cache version
        // TODO: if this necessary?
        PixelsZoneUtil.setIndexVersion(globalIndexFile, version);
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
            throws IOException, InterruptedException {
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
        // this.cachedColumnChunks.clear();
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
                long bucketId = locator.getLocation(new PixelsCacheKey(blockId, rowGroupId, columnId));
                // TODO: if lock is needed here?
                int zoneId = bucketToZoneMap.getBucketToZone(bucketId);
                PixelsZoneWriter zone = zones.get(zoneId);
                PixelsCacheIdx curCacheIdx = zone.getRadix().get(blockId, rowGroupId, columnId);
                survivedIdxes.add(
                        new PixelsCacheEntry(new PixelsCacheKey(
                                physicalReader.getCurrentBlockId(), rowGroupId, columnId), curCacheIdx));
            }
        }
        // ascending order according to the offset in cache file.
        Collections.sort(survivedIdxes);

        for(int j=0; j<bucketTypeInfo.getLazyBucketNum(); ++j)
        {
            /**
             * Start phase 1: compact the survived cache elements.
             */
            logger.debug("Start cache compaction...");
            long newCacheOffset = PixelsZoneUtil.ZONE_DATA_OFFSET;
            // get origin lazy zone
            int bucketId = bucketTypeInfo.getLazyBucketIds().get(j);
            // TODO: if lock is needed here?
            int zoneId = bucketToZoneMap.getBucketToZone(bucketId);
            PixelsZoneWriter zone = zones.get(zoneId);
            // get swap zone
            int swapBucketId = bucketTypeInfo.getSwapBucketIds().get(0);
            try
            {
                bucketToZoneMap.beginWrite(swapBucketId);
            }catch (InterruptedException e)
            {
                status = -1;
                logger.error("Failed to get write permission on bucketToZoneMap.", e);
                return status;
            }
            int swapZoneId = bucketToZoneMap.getBucketToZone(swapBucketId);
            PixelsZoneWriter swapZone = zones.get(swapZoneId);
            MemoryMappedFile swapIndexFile = swapZone.getIndexFile();
            // copy the survived cache elements to the swap zone.
            for(PixelsCacheEntry survivedIdx:survivedIdxes)
            {
                if(locator.getLocation(survivedIdx.key) != bucketId){
                    continue;
                }
                swapZone.getZoneFile().copyMemory(survivedIdx.idx.offset, newCacheOffset, survivedIdx.idx.length);
                swapZone.getRadix().put(survivedIdx.key, new PixelsCacheIdx(newCacheOffset, survivedIdx.idx.length));
                newCacheOffset += survivedIdx.idx.length;
            }
            swapZone.flushIndex();
            PixelsZoneUtil.setStatus(swapZone.getZoneFile(), PixelsZoneUtil.ZoneStatus.OK.getId());
            PixelsZoneUtil.setSize(swapZone.getZoneFile(), newCacheOffset);
            swapZone.changeZoneTypeS2L();
            logger.debug("Cache compaction to bucket " + bucketId + " finished");

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
                    if(locator.getLocation(new PixelsCacheKey(pixelsPhysicalReader.getCurrentBlockId(), rowGroupId, columnId)) != bucketId){
                        continue;
                    }
                    PixelsProto.RowGroupFooter rowGroupFooter = pixelsPhysicalReader.readRowGroupFooter(rowGroupId);
                    PixelsProto.ColumnChunkIndex chunkIndex =
                            rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntries(columnId);
                    physicalLen = chunkIndex.getChunkLength();
                    physicalOffset = chunkIndex.getChunkOffset();
                    if (newCacheOffset + physicalLen >= swapZone.getZoneFile().getSize())
                    {
                        logger.debug("Cache writes have exceeded bucket size. Break. Current size: " + newCacheOffset + "current bucketId: " + bucketId);
                        status = 2;
                        break outer_loop;
                    }
                    else
                    {
                        newIdxes.add(new PixelsCacheEntry(
                                new PixelsCacheKey(pixelsPhysicalReader.getCurrentBlockId(), rowGroupId, columnId),
                                new PixelsCacheIdx(newCacheOffset, physicalLen)));
                        byte[] columnChunk = pixelsPhysicalReader.read(physicalOffset, physicalLen);
                        swapZone.getZoneFile().setBytes(newCacheOffset, columnChunk);
                        logger.debug(
                                "Cache write: " + file + "-" + rowGroupId + "-" + columnId + ", offset: " + newCacheOffset + ", length: " + columnChunk.length);
                        newCacheOffset += physicalLen;
                    }
                }
            }
            logger.debug("Bucket " + bucketId + " append finished, writer ends at offset: " + newCacheOffset);


            /**
             * Start phase 3: activate new cache elements.
             */
            logger.debug("Start activating new cache elements...");
            for (PixelsCacheEntry newIdx : newIdxes)
            {
                swapZone.getRadix().put(newIdx.key, newIdx.idx);
            }
            // flush index
            swapZone.flushIndex();
            // update cache version
            PixelsCacheUtil.setIndexVersion(swapZone.getIndexFile(), version);
            PixelsCacheUtil.setCacheStatus(swapZone.getZoneFile(), PixelsCacheUtil.CacheStatus.OK.getId());
            PixelsCacheUtil.setCacheSize(swapZone.getZoneFile(), newCacheOffset);

            // update bucketToZoneMap to let this bucketId point to the old swap zone.
            bucketToZoneMap.updateBucketZoneMap(bucketId,swapZoneId);
            // change zone type from lazy to swap
            try
            {
                PixelsZoneUtil.beginIndexWrite(zone.getIndexFile());
            }catch (InterruptedException e)
            {
                status = -1;
                logger.error("Failed to get write permission on bucketToZoneMap.", e);
                return status;
            }
            zone.changeZoneTypeL2S();
            PixelsCacheUtil.setIndexVersion(zone.getIndexFile(), version);
            PixelsZoneUtil.endIndexWrite(zone.getIndexFile());
            bucketToZoneMap.updateBucketZoneMap(swapBucketId,zoneId);
            bucketToZoneMap.endWrite(swapBucketId);
        }
        // TODO: if moved here correctly?
        this.cachedColumnChunks.clear();
        this.cachedColumnChunks.addAll(survivedColumnChunks);
        // save the new cached column chunks into cachedColumnChunks.
        for (String newColumnChunk : newCachedColumnChunks)
        {
            this.cachedColumnChunks.add(newColumnChunk);
        }
        // TODO: if this necessary?
        // update cache version
        PixelsZoneUtil.setIndexVersion(globalIndexFile, version);
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
