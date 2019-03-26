package com.x.common.flume.sink.mysql;

import com.x.common.flume.sink.OneMonthRollingMySQLSink;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * 为 {@link OneMonthRollingMySQLSink} 提供数据库访问支持。
 * 提供 {@link OneDayRollingMySQLContext#comApiLogDao()} 方法返回 dao 实例
 * Created by jonas on 2017/4/21.
 */
@SpringBootApplication
public class OneDayRollingMySQLContext {

    private static ConfigurableApplicationContext context;

    public static void startup() {
        context = SpringApplication.run(OneDayRollingMySQLContext.class);
    }

    public static void shutdown() {
        context.close();
    }

    public static ComApiLogDao comApiLogDao() {
        return context.getBean(ComApiLogDao.class);
    }
}
