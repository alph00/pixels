package cn.edu.ruc.iir.pixels.daemon.cache;

import cn.edu.ruc.iir.pixels.cache.PixelsCacheConfig;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheUtil;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheWriter;
import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.utils.Constants;
import cn.edu.ruc.iir.pixels.common.utils.EtcdUtil;
import cn.edu.ruc.iir.pixels.daemon.Server;
import com.coreos.jetcd.Lease;
import com.coreos.jetcd.Watch;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * pixels cache manager.
 *
 * @author guodong
 */
public class CacheManager
        implements Server
{
    private static Logger logger = LoggerFactory.getLogger(CacheManager.class);
    // cache status: initializing(0), ready(1), updating(2), dead(-1)
    private static AtomicInteger cacheStatus = new AtomicInteger(0);

    private PixelsCacheWriter cacheWriter = null;
    private MetadataService metadataService = null;
    private CacheManagerStatusRegister cacheStatusRegister;
    private final PixelsCacheConfig cacheConfig;
    private final EtcdUtil etcdUtil;
    private final ScheduledExecutorService scheduledExecutor;

    public CacheManager()
    {
        this.cacheConfig = new PixelsCacheConfig();
        this.etcdUtil = EtcdUtil.Instance();
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        initialize();
    }

    /**
     * Initialize CacheManager
     *
     * 1. check if cache file exists.
     *    if exists, check if existing cache version is the same as current cache version in etcd.
     *      if not, existing cache is out of date, goto step #2.
     * 2. else, update caches with latest layout in etcd/mysql.
     * 3. update the status of CacheManager in etcd
     * 4. start a scheduled thread to update node(CacheManager) status
     * 5. add a watcher to listen to changes of the cache version in etcd.
     *    if there is a new version, we need update caches according to new layouts.
     * */
    private void initialize()
    {
        try {
            this.cacheWriter =
                    PixelsCacheWriter.newBuilder()
                                     .setCacheLocation(cacheConfig.getCacheLocation())
                                     .setCacheSize(cacheConfig.getCacheSize())
                                     .setIndexLocation(cacheConfig.getIndexLocation())
                                     .setIndexSize(cacheConfig.getIndexSize())
                                     .setOverwrite(false).build();
            this.metadataService = new MetadataService(cacheConfig.getMetaHost(), cacheConfig.getMetaPort());
            int localCacheVersion = PixelsCacheUtil.getIndexVersion(cacheWriter.getIndexFile());
            KeyValue globalCacheVersionKV = etcdUtil.getKeyValue(Constants.CACHE_VERSION_LITERAL);
            // if global cache version does not exist, maybe coordinator has not started normally yet.
            if (globalCacheVersionKV == null) {
                // cache coordinator has not started yet. exit.
                return;
            }
            int globalCacheVersion = Integer.parseInt(globalCacheVersionKV.getValue().toStringUtf8());
            // if cache file exists already. we need check local cache version with global cache version stored in etcd
            if (localCacheVersion >= 0) {
                // if global version is not consistent with the local one. update local cache.
                if (globalCacheVersion != localCacheVersion) {
                    // update local cache
                    update(globalCacheVersion);
                }
            }
            // if this is a fresh start of local cache, then update local cache to match the global one
            else {
                // update local cache
                update(globalCacheVersion);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            return;
        }
        Lease leaseClient = etcdUtil.getClient().getLeaseClient();
        // get a lease from etcd with a specified ttl, add this caching node into etcd with a granted lease
        try {
            long leaseId = leaseClient.grant(cacheConfig.getNodeLeaseTTL()).get(10, TimeUnit.SECONDS).getID();
            etcdUtil.putKeyValueWithLeaseId(Constants.CACHE_NODE_STATUS_LITERAL + cacheConfig.getHostAddress(),
                                            "" + cacheStatus.get(), leaseId);
            // start a scheduled thread to update node status periodically
            this.cacheStatusRegister = new CacheManagerStatusRegister(leaseClient, leaseId);
            scheduledExecutor.scheduleAtFixedRate(cacheStatusRegister, 1, 10, TimeUnit.SECONDS);
            cacheStatus.set(1);
            etcdUtil.putKeyValue(Constants.CACHE_NODE_STATUS_LITERAL + cacheConfig.getHostAddress(), "" + cacheStatus.get());
        }
        // registration failed with exceptions.
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void update(int version)
            throws MetadataException
    {
        // todo meta server should provide a interface to get layouts with version filtering
        List<Layout> layouts = metadataService.getLayouts(cacheConfig.getSchema(), cacheConfig.getTable());
        Set<Layout> matchedLayouts = layouts.stream().filter(
                l -> l.getVersion() == version).collect(Collectors.toSet());
        if (!matchedLayouts.isEmpty()) {
            // update cache status
            cacheStatus.set(2);
            etcdUtil.putKeyValue(Constants.CACHE_NODE_STATUS_LITERAL + cacheConfig.getHostAddress(), "" + cacheStatus.get());
            // update cache content
            if (cacheWriter.updateAll(version, matchedLayouts.iterator().next())) {
                cacheStatus.set(1);
                etcdUtil.putKeyValue(Constants.CACHE_NODE_STATUS_LITERAL + cacheConfig.getHostAddress(), "" + cacheStatus.get());
            }
            else {
                // todo deal with exceptions when local cache update failed
            }
        }
    }

    @Override
    public void run()
    {
        Watch watch = etcdUtil.getClient().getWatchClient();
        Watch.Watcher watcher = watch.watch(
                ByteSequence.fromString(Constants.CACHE_VERSION_LITERAL), WatchOption.DEFAULT);
        while (cacheStatus.get() > 0) {
            try {
                WatchResponse watchResponse = watcher.listen();
                for (WatchEvent event : watchResponse.getEvents()) {
                    // update a new version
                    if (event.getEventType() == WatchEvent.EventType.PUT) {
                        int version = Integer.parseInt(event.getKeyValue().getValue().toStringUtf8());
                        update(version);
                    }
                    else {
                        logger.error("Unknown changes watched on cache version");
                        break;
                    }
                }
            }
            catch (InterruptedException | MetadataException e) {
                logger.error(e.getMessage());
                e.printStackTrace();
                break;
            }
        }
    }

    @Override
    public boolean isRunning()
    {
        return cacheStatus.get() >= 0;
    }

    @Override
    public void shutdown()
    {
        cacheStatus.set(-1);
        etcdUtil.putKeyValue(Constants.CACHE_NODE_STATUS_LITERAL + cacheConfig.getHostAddress(), "" + cacheStatus.get());
        cacheStatusRegister.stop();
        this.scheduledExecutor.shutdownNow();
    }

    /**
     * Scheduled register to update caching node status and keep its registration alive.
     * */
    private static class CacheManagerStatusRegister
            implements Runnable
    {
        private final Lease leaseClient;
        private final long leaseId;

        CacheManagerStatusRegister(Lease leaseClient, long leaseId)
        {
            this.leaseClient = leaseClient;
            this.leaseId = leaseId;

        }

        @Override
        public void run()
        {
            try {
                leaseClient.keepAliveOnce(leaseId);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void stop()
        {
            leaseClient.revoke(leaseId);
            leaseClient.close();
        }
    }
}
