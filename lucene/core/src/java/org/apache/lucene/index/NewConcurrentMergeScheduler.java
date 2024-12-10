package org.apache.lucene.index;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;

import java.io.IOException;

public class NewConcurrentMergeScheduler extends ThreadPoolMergeScheduler {

    /**
     * Dynamic default for {@code maxThreadCount} and {@code maxMergeCount}, based on CPU core count.
     * {@code maxThreadCount} is set to {@code max(1, min(4, cpuCoreCount/2))}. {@code maxMergeCount}
     * is set to {@code maxThreadCount + 5}.
     */
    public static final int AUTO_DETECT_MERGES_AND_THREADS = -1;

    /**
     * Used for testing.
     *
     * @lucene.internal
     */
    public static final String DEFAULT_CPU_CORE_COUNT_PROPERTY = "lucene.cms.override_core_count";

    /**
     * How many {@link ConcurrentMergeScheduler.MergeThread}s have kicked off (this is use to name them).
     */
    protected int mergeThreadCount;

    /**
     * Create and return a new MergeThread
     */
    protected synchronized Thread getMergeThread(MergeTask mergeTask)
            throws IOException {
        final Thread thread = new Thread(mergeTask);
        thread.setDaemon(true);
        thread.setName("Lucene Merge Thread #" + mergeThreadCount++);
        return thread;
    }

    @Override
    protected void dispatchMerge(MergeTask newMergeTask) throws IOException {
        getMergeThread(newMergeTask).run();
    }

    @Override
    void initialize(InfoStream infoStream, Directory directory) throws IOException {
        super.initialize(infoStream, directory);
    }
}
