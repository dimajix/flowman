package com.dimajix.flowman.kernel;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThreadPoolExecutor {
    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolExecutor.class);

    private static class MyForkJoinWorkerThread extends ForkJoinWorkerThread { // set the correct classloader here
        public MyForkJoinWorkerThread(ForkJoinPool pool) {
            super(pool);
            setContextClassLoader(Thread.currentThread().getContextClassLoader());
        }
    }
    private static class MyForkJoinWorkerThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {
        public ForkJoinWorkerThread newThread(ForkJoinPool pool) { return new MyForkJoinWorkerThread(pool); }
    }
    public static Executor newExecutor() {
        val exceptionHandler = new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread thread, Throwable throwable) {
                logger.error("Uncaught exception: ", throwable);
            }
        };
        return new ForkJoinPool(4, new MyForkJoinWorkerThreadFactory(), exceptionHandler, true);
    }
}
