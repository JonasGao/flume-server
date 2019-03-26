package com.x.common.components.logging.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.Date;
import java.util.Properties;

@SpringBootApplication
@ComponentScan("com.x")
public class FlumeApiLogClientTestApplication implements CommandLineRunner {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void run(String... args) throws Exception {

        String host = "127.0.0.1:44448";

        if (args.length > 0) {
            host = args[0];
        }

        logger.info("准备连接 {}", host);

        Properties props = new Properties();
        props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
        props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "h1", host);
        props.setProperty(RpcClientConfigurationConstants.CONFIG_CONNECT_TIMEOUT, String.valueOf(10000));
        props.setProperty(RpcClientConfigurationConstants.CONFIG_REQUEST_TIMEOUT, String.valueOf(10000));
        RpcClient rpcClient = RpcClientFactory.getInstance(props);

        logger.info("已连接");

        ComApiLogModel comApiLogModel = new ComApiLogModel();
        comApiLogModel.setApimethodname("apimethodname");
        comApiLogModel.setChannelId("010");
        comApiLogModel.setCartId(100);
        comApiLogModel.setCallTime("callTime");
        comApiLogModel.setTimeZone("timeZone");
        comApiLogModel.setUsername("username");
        comApiLogModel.setComputerName("computerName");
        comApiLogModel.setIp("ip");
        comApiLogModel.setMac("mac");
        comApiLogModel.setActive(true);
        comApiLogModel.setThirdPart("vo");

        comApiLogModel.setCreated(new Date());
        comApiLogModel.setModified(new Date());
        comApiLogModel.setCreater("Jonas With TestHelper");
        comApiLogModel.setModifier("Jonas With TestHelper");

        String json = new ObjectMapper().writeValueAsString(comApiLogModel);
        Event flumeEvent = EventBuilder.withBody(json, Charsets.UTF_8);

        logger.info("已序列化信息");

        rpcClient.append(flumeEvent);

        logger.info("已发送测试信息");

        logger.info("关闭连接");

        rpcClient.close();
    }

    public static void main(String[] args) {
        SpringApplication.run(FlumeApiLogClientTestApplication.class, args);
    }
}
