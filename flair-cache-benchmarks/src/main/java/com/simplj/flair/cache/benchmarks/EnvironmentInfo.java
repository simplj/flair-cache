package com.simplj.flair.cache.benchmarks;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class EnvironmentInfo {

    final String jvmVersion;
    final String jvmVendor;
    final String jvmName;
    final String osName;
    final String osVersion;
    final String osArch;
    final int availableProcessors;
    final long maxHeapBytes;
    final List<String> gcAlgorithms;
    final List<String> jvmInputArguments;

    private EnvironmentInfo(
            String jvmVersion, String jvmVendor, String jvmName,
            String osName, String osVersion, String osArch,
            int availableProcessors, long maxHeapBytes,
            List<String> gcAlgorithms, List<String> jvmInputArguments) {
        this.jvmVersion = jvmVersion;
        this.jvmVendor = jvmVendor;
        this.jvmName = jvmName;
        this.osName = osName;
        this.osVersion = osVersion;
        this.osArch = osArch;
        this.availableProcessors = availableProcessors;
        this.maxHeapBytes = maxHeapBytes;
        this.gcAlgorithms = Collections.unmodifiableList(gcAlgorithms);
        this.jvmInputArguments = Collections.unmodifiableList(jvmInputArguments);
    }

    static EnvironmentInfo capture() {
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long maxHeap = heap.getMax() >= 0 ? heap.getMax() : Runtime.getRuntime().maxMemory();

        List<String> gcNames = new ArrayList<>();
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            gcNames.add(gc.getName());
        }

        return new EnvironmentInfo(
                System.getProperty("java.version"),
                System.getProperty("java.vendor"),
                System.getProperty("java.vm.name"),
                System.getProperty("os.name"),
                System.getProperty("os.version"),
                System.getProperty("os.arch"),
                Runtime.getRuntime().availableProcessors(),
                maxHeap,
                gcNames,
                new ArrayList<>(ManagementFactory.getRuntimeMXBean().getInputArguments())
        );
    }

    void printSummary() {
        System.out.println("=== Benchmark Environment ===");
        System.out.printf("JVM:       %s %s (%s)%n", jvmName, jvmVersion, jvmVendor);
        System.out.printf("OS:        %s %s [%s]%n", osName, osVersion, osArch);
        System.out.printf("CPUs:      %d%n", availableProcessors);
        if (maxHeapBytes == Long.MAX_VALUE || maxHeapBytes < 0) {
            System.out.println("Max heap:  unlimited");
        } else {
            System.out.printf("Max heap:  %.1f MB%n", maxHeapBytes / (1024.0 * 1024.0));
        }
        System.out.printf("GC:        %s%n", String.join(", ", gcAlgorithms));
        if (!jvmInputArguments.isEmpty()) {
            System.out.printf("JVM args:  %s%n", String.join(" ", jvmInputArguments));
        }
        System.out.println("=============================================================");
        System.out.println();
    }

    void appendJson(StringBuilder sb) {
        sb.append("  \"environment\": {\n");
        sb.append("    \"jvmVersion\": ").append(q(jvmVersion)).append(",\n");
        sb.append("    \"jvmVendor\": ").append(q(jvmVendor)).append(",\n");
        sb.append("    \"jvmName\": ").append(q(jvmName)).append(",\n");
        sb.append("    \"osName\": ").append(q(osName)).append(",\n");
        sb.append("    \"osVersion\": ").append(q(osVersion)).append(",\n");
        sb.append("    \"osArch\": ").append(q(osArch)).append(",\n");
        sb.append("    \"availableProcessors\": ").append(availableProcessors).append(",\n");
        sb.append("    \"maxHeapBytes\": ").append(maxHeapBytes).append(",\n");
        sb.append("    \"gcAlgorithms\": [");
        for (int i = 0; i < gcAlgorithms.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(q(gcAlgorithms.get(i)));
        }
        sb.append("],\n");
        sb.append("    \"jvmInputArguments\": [");
        for (int i = 0; i < jvmInputArguments.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(q(jvmInputArguments.get(i)));
        }
        sb.append("]\n");
        sb.append("  }");
    }

    private static String q(String s) {
        if (s == null) return "null";
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
