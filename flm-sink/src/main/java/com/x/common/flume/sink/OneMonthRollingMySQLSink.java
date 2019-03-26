package com.x.common.flume.sink;

import com.google.common.base.Charsets;
import com.x.common.flume.sink.mysql.ComApiLogDao;
import com.x.common.flume.sink.mysql.OneDayRollingMySQLContext;
import com.x.ecerp.model.common.ComApiLogModel;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 接收并处理 {@link Event}，使用 {@link OneDayRollingMySQLContext#comApiLogDao()} 提供的 dao 将处理后的
 * {@link ComApiLogModel} 插入数据库，对所有的 api 日志进行统一收集。
 * <p>
 *     Rolling 的操作并不像文件那样，因为 {@link ComApiLogModel} 自身提供了
 *     {@link ComApiLogModel#modified} 字段，所以不需要这里显式 Rolling 操作。
 *     参见 {@link ComApiLogDao#getTableName(ComApiLogModel)}
 * </p>
 * Created by jonas on 2017/4/19.
 */
public class OneMonthRollingMySQLSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(OneMonthRollingMySQLSink.class);
    private static final int defaultBatchSize = 100;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private Integer batchSize = defaultBatchSize;
    private SinkCounter sinkCounter;
    private ComApiLogDao comApiLogDao;

    @Override
    public synchronized void start() {
        logger.info("Starting {}...", this);
        sinkCounter.start();
        super.start();
        OneDayRollingMySQLContext.startup();
    }

    @Override
    public synchronized void stop() {
        logger.info("OneMonthRollingMySQLSink sink {} stopping...", getName());
        sinkCounter.stop();
        super.stop();
        OneDayRollingMySQLContext.shutdown();
        logger.info("OneMonthRollingMySQLSink sink {} stopped. Event metrics: {}", getName(), sinkCounter);
    }

    @Override
    public Status process() throws EventDeliveryException {

        if (comApiLogDao == null) {
            comApiLogDao = OneDayRollingMySQLContext.comApiLogDao();
        }

        if (comApiLogDao == null) {
            throw new GetComApiLogDaoFailException();
        }

        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Status result = Status.READY;
        try {
            transaction.begin();
            int eventAttemptCounter = 0;
            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event != null) {

                    // 在开始处理前，递增成功 take 计数
                    sinkCounter.incrementEventDrainAttemptCount();
                    String json = null;

                    try {
                        json = new String(event.getBody(), Charsets.UTF_8);
                        ComApiLogModel comApiLogModel = objectMapper.readValue(json, ComApiLogModel.class);
                        comApiLogDao.insert(comApiLogModel);
                        // 在成功后才增加成功计数
                        eventAttemptCounter++;
                    } catch (Exception e) {
                        logger.warn("反序列化数据并插入数据库时出现错误 {}", json);
                        logger.warn("反序列化数据并插入数据库时的错误异常", e);
                    }

                } else {
                    // No events found, request back-off semantics from runner
                    result = Status.BACKOFF;
                    break;
                }
            }

            transaction.commit();
            sinkCounter.addToEventDrainSuccessCount(eventAttemptCounter);
        } catch (Exception ex) {
            transaction.rollback();
            throw new EventDeliveryException("Failed to process transaction", ex);
        } finally {
            transaction.close();
        }

        return result;
    }

    @Override
    public void configure(Context context) {
        batchSize = context.getInteger("sink.batchSize", defaultBatchSize);
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    public class GetComApiLogDaoFailException extends EventDeliveryException {
        GetComApiLogDaoFailException() {
            super("从 OneDayRollingMySQLContext 中获取 ComApiLogDao 为 null，请检查上下文配置");
        }
    }
}
