package com.x.common.flume.sink;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.x.common.components.logging.MDCKey;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * Created by jonas on 2017/4/27.
 *
 * @version 1.0
 * @since 0.1.20170417162650-SNAPSHOT
 */
public class OneDayRollingMultiFileSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory
            .getLogger(CmsOneDayRollingMultiFileSink.class);
    private static final int defaultBatchSize = 100;
    private static final String UN_KNOWN = "unknown";

    private Context serializerContext;
    private Integer batchSize = defaultBatchSize;
    private File directory;
    private SinkCounter sinkCounter;
    private ScheduledExecutorService rollService;
    private String serializerType;

    private Map<String, FileContext> targetContextMap = new ConcurrentHashMap<>();

    @Override
    public synchronized void start() {
        logger.info("Starting {}...", this);
        sinkCounter.start();
        super.start();

        rollService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat(
                "oneDayMultiTargetRollingFileSink-roller-" + Thread.currentThread().getId() + "-%d").build());

        ZoneOffset utcZoneOffset = ZoneOffset.UTC;

        long tomorrowUTCFirstSecond = LocalDateTime.now()
                .plusDays(1)
                .withHour(0)
                .withMinute(0)
                .withSecond(1)
                .withNano(0)
                .toInstant(utcZoneOffset)
                .toEpochMilli();

        long delayTomorrowUTCFirstSecond = tomorrowUTCFirstSecond - LocalDateTime.now().toInstant(utcZoneOffset).toEpochMilli();

        long oneDay = 24 * 60 * 60 * 1000;

        /*
         * Every N seconds, mark that it's time to rotate. We purposefully do NOT
         * touch anything other than the indicator flag to avoid error handling
         * issues (e.g. IO exceptions occuring in two different threads.
         * Resist the urge to actually perform rotation in a separate thread!
         */
        rollService.scheduleAtFixedRate(() -> targetContextMap.values()
                        .forEach(targetContext -> targetContext.shouldRotate = true),
                delayTomorrowUTCFirstSecond, oneDay, TimeUnit.MILLISECONDS);

        logger.info("OneDayMultiTargetRollingFileSink {} started.", getName());
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        Status result = Status.READY;

        try {
            transaction.begin();
            int eventAttemptCounter = 0;
            for (int i = 0; i < batchSize; i++) {
                event = channel.take();
                if (event != null) {

                    sinkCounter.incrementEventDrainAttemptCount();
                    eventAttemptCounter++;

                    write(event);

                } else {
                    // No events found, request back-off semantics from runner
                    result = Status.BACKOFF;
                    break;
                }
            }

            transaction.commit();
            sinkCounter.addToEventDrainSuccessCount(eventAttemptCounter);
        } catch (EventDeliveryException ed) {
            transaction.rollback();
            throw ed;
        } catch (Exception ex) {
            transaction.rollback();
            throw new EventDeliveryException("Failed to process transaction", ex);
        } finally {
            transaction.close();
        }

        return result;
    }

    @Override
    public synchronized void stop() {
        logger.info("RollingFile sink {} stopping...", getName());
        sinkCounter.stop();
        super.stop();

        targetContextMap.values().forEach(FileContext::stop);

        if (rollService != null) {
            rollService.shutdown();

            while (!rollService.isTerminated()) {
                try {
                    rollService.awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger.debug("Interrupted while waiting for roll service to stop. " +
                            "Please report this.", e);
                }
            }
        }
        logger.info("RollingFile sink {} stopped. Event metrics: {}",
                getName(), sinkCounter);
    }

    @Override
    public void configure(Context context) {
        String directory = context.getString("sink.directory");

        serializerType = context.getString("sink.serializer", "TEXT");

        serializerContext =
                new Context(context.getSubProperties("sink." +
                        EventSerializer.CTX_PREFIX));

        Preconditions.checkArgument(directory != null, "Directory may not be null");
        Preconditions.checkNotNull(serializerType, "Serializer type is undefined");

        batchSize = context.getInteger("sink.batchSize", defaultBatchSize);
        this.directory = new File(directory);

        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    void write(Event event) throws Exception {

        Map<String, String> headers = event.getHeaders();

        // 上层目录参数
        final String subSystem = headers.getOrDefault(MDCKey.subSystem.name(), UN_KNOWN);
        final String caller = headers.getOrDefault(MDCKey.caller.name(), UN_KNOWN);
        final String creator = headers.getOrDefault(MDCKey.creator.name(), UN_KNOWN);

        final List<String> targets = new ArrayList<>();

        // 格式为: subSystem/(caller|creator)/(caller|creator)-yyyy-MM-dd.log
        // 日期和后缀由 PathManager 处理
        targets.add(join(new String[]{subSystem, creator, creator}, "/"));
        if (!caller.equals(UN_KNOWN) && !caller.equals(creator)) {
            targets.add(join(new String[]{subSystem, caller, caller}, "/"));
        }

        // 如果有，继续组装下层细分目录
        final String taskName = headers.get(MDCKey.taskName.name());
        if (isNotBlank(taskName)) {
            // 有 taskName 的最终输出目录
            // 格式为: subSystem/(caller|creator)/tasks/taskName/taskName-yyyy-MM-dd.log
            // 日期和后缀由 PathManager 处理
            targets.add(join(new String[]{subSystem, creator, "tasks", taskName, taskName}, "/"));
            if (!caller.equals(UN_KNOWN) && !caller.equals(creator)) {
                targets.add(join(new String[]{subSystem, caller, "tasks", taskName, taskName}, "/"));
            }
        }

        for (String target : targets) {
            context(target).write(event);
        }
    }

    FileContext context(String key) {
        return targetContextMap.computeIfAbsent(key, FileContext::new);
    }

    public class FileContext {
        boolean shouldRotate = false;
        EventSerializer serializer;
        OneDayRollingPathManager pathManager;
        BufferedOutputStream outputStream;

        FileContext(String fileName) {
            pathManager = new OneDayRollingPathManager(fileName, directory);
        }

        void mkdirs() {
            File parentFile = pathManager.getCurrentFile().getParentFile();
            if (!parentFile.mkdirs() && !parentFile.exists()) {
                throw new MkdirException(parentFile);
            }
        }

        void rotate() throws EventDeliveryException {
            if (outputStream == null) {
                return;
            }
            logger.debug("Closing file {}", pathManager.getCurrentFile());
            try {
                serializer.flush();
                serializer.beforeClose();
                outputStream.close();
                sinkCounter.incrementConnectionClosedCount();
                shouldRotate = false;
            } catch (IOException e) {
                sinkCounter.incrementConnectionFailedCount();
                throw new EventDeliveryException("Unable to rotate file "
                        + pathManager.getCurrentFile() + " while delivering event", e);
            } finally {
                serializer = null;
                outputStream = null;
            }
            pathManager.rotate();
        }

        void write(Event event) throws EventDeliveryException, IOException {
            if (shouldRotate) {
                logger.debug("Time to rotate {}", pathManager.getCurrentFile());
                rotate();
            }

            if (outputStream == null) {
                mkdirs();
                File currentFile = pathManager.getCurrentFile();
                logger.debug("Opening output stream for file {}", currentFile);
                try {
                    outputStream = new BufferedOutputStream(
                            new FileOutputStream(currentFile, true));
                    serializer = EventSerializerFactory.getInstance(
                            serializerType, serializerContext, outputStream);
                    serializer.afterCreate();
                    sinkCounter.incrementConnectionCreatedCount();
                } catch (IOException e) {
                    sinkCounter.incrementConnectionFailedCount();
                    throw new EventDeliveryException("Failed to open file "
                            + pathManager.getCurrentFile() + " while delivering event", e);
                }
            }

            serializer.write(event);
            outputStream.flush();
        }

        void stop() {
            if (outputStream != null) {
                logger.debug("Closing file {}", pathManager.getCurrentFile());

                try {
                    serializer.flush();
                    serializer.beforeClose();
                    outputStream.close();
                    sinkCounter.incrementConnectionClosedCount();
                } catch (IOException e) {
                    sinkCounter.incrementConnectionFailedCount();
                    logger.error("Unable to close output stream. Exception follows.", e);
                } finally {
                    outputStream = null;
                    serializer = null;
                }
            }
        }
    }

    public class MkdirException extends RuntimeException {
        MkdirException(File parentFile) {
            super("创建日志目录失败: " + parentFile.getAbsolutePath());
        }
    }
}
