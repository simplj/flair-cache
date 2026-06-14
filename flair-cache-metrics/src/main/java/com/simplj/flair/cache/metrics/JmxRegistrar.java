package com.simplj.flair.cache.metrics;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

final class JmxRegistrar {

    private static final Logger log = Logger.getLogger(JmxRegistrar.class.getName());

    static final String DOMAIN = "com.simplj.flair.cache";

    private final MBeanServer  mbs        = ManagementFactory.getPlatformMBeanServer();
    // ConcurrentHashMap-backed Set: O(1) add/remove, no duplicates, and safe under concurrent
    // register()/deregisterAll() races. Set semantics prevent the same ObjectName from
    // appearing twice when the same register*Metrics() method is called more than once.
    private final Set<ObjectName> registered = ConcurrentHashMap.newKeySet();

    void registerCacheMetrics(String blockName, CacheMetricsMBean bean) {
        register(bean, objectName("type=CacheMetrics,block=" + quoteName(blockName)));
    }

    void registerReplicationMetrics(ReplicationMetricsMBean bean) {
        register(bean, objectName("type=ReplicationMetrics"));
    }

    void registerClusterMetrics(ClusterMetricsMBean bean) {
        register(bean, objectName("type=ClusterMetrics"));
    }

    void registerEvictionMetrics(EvictionMetricsMBean bean) {
        register(bean, objectName("type=EvictionMetrics"));
    }

    void deregisterCacheMetrics(String blockName) {
        deregister(objectName("type=CacheMetrics,block=" + quoteName(blockName)));
    }

    void deregisterAll() {
        // Remove each name from the set before unregistering it (same ordering as deregister()).
        // This prevents a register() call that races between the end of iteration and a bulk clear()
        // from having its ObjectName cleared without the corresponding MBean being deregistered.
        for (Iterator<ObjectName> it = registered.iterator(); it.hasNext(); ) {
            ObjectName name = it.next();
            it.remove();
            try {
                if (mbs.isRegistered(name)) mbs.unregisterMBean(name);
            } catch (Exception e) {
                log.log(Level.WARNING, "Failed to deregister MBean: " + name, e);
            }
        }
    }

    private void register(Object bean, ObjectName name) {
        try {
            if (mbs.isRegistered(name)) mbs.unregisterMBean(name);
            mbs.registerMBean(bean, name);
            registered.add(name); // Set.add() is a no-op if name is already tracked
        } catch (Exception e) {
            log.log(Level.WARNING, "Failed to register MBean: " + name, e);
        }
    }

    private void deregister(ObjectName name) {
        registered.remove(name); // stop tracking before unregistering so deregisterAll() won't retry
        try {
            if (mbs.isRegistered(name)) mbs.unregisterMBean(name);
        } catch (Exception e) {
            log.log(Level.WARNING, "Failed to deregister MBean: " + name, e);
        }
    }

    private ObjectName objectName(String props) {
        try {
            return new ObjectName(DOMAIN + ":" + props);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid MBean ObjectName props: " + props, e);
        }
    }

    // Quote block names that contain JMX ObjectName special characters
    private static String quoteName(String name) {
        if (name.matches("[a-zA-Z0-9_.-]+")) return name;
        return ObjectName.quote(name);
    }
}
