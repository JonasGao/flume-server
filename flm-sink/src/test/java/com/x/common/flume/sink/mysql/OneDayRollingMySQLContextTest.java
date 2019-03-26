package com.x.common.flume.sink.mysql;

import com.x.ecerp.model.common.ComApiLogModel;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by jonas on 2017/5/2.
 */
public class OneDayRollingMySQLContextTest {
    @Test
    public void testStartup() {
        OneDayRollingMySQLContext.startup();
        ComApiLogDao comApiLogDao = OneDayRollingMySQLContext.comApiLogDao();
        assertNotNull(comApiLogDao);
        OneDayRollingMySQLContext.shutdown();
    }

    @Test
    public void testGetTableName() {
        OneDayRollingMySQLContext.startup();
        ComApiLogDao comApiLogDao = OneDayRollingMySQLContext.comApiLogDao();

        ComApiLogModel comApiLogModel = new ComApiLogModel();
        comApiLogModel.setApimethodname("测试接口");
        comApiLogModel.setChannelId("test channel");
        comApiLogModel.setCartId(99);
        comApiLogModel.setCallTime(LocalDateTime.now().toString());
        comApiLogModel.setTimeZone("shanghai");
        comApiLogModel.setUsername("somebody");
        comApiLogModel.setComputerName("test computer");
        comApiLogModel.setIp("10.0.0.1");
        comApiLogModel.setMac("10:10:10:10:10:10");
        comApiLogModel.setActive(true);
        comApiLogModel.setThirdPart("测试方");

        String tableName = comApiLogDao.getTableName(comApiLogModel);
        System.out.println(tableName);
        assertEquals("com_api_log_测试方_20170502", tableName);

        OneDayRollingMySQLContext.shutdown();
    }

    @Test
    public void testInsert() {
        OneDayRollingMySQLContext.startup();
        ComApiLogDao comApiLogDao = OneDayRollingMySQLContext.comApiLogDao();

        ComApiLogModel comApiLogModel = new ComApiLogModel();
        comApiLogModel.setApimethodname("测试接口");
        comApiLogModel.setChannelId("999");
        comApiLogModel.setCartId(99);
        comApiLogModel.setCallTime(LocalDateTime.now().toString());
        comApiLogModel.setTimeZone("shanghai");
        comApiLogModel.setUsername("somebody");
        comApiLogModel.setComputerName("test computer");
        comApiLogModel.setIp("10.0.0.1");
        comApiLogModel.setMac("10:10:10:10:10:10");
        comApiLogModel.setActive(true);
        comApiLogModel.setThirdPart("taobao");

        comApiLogModel.setCreater("测试");
        comApiLogModel.setModifier("测试");
        comApiLogModel.setCreated(new Date());
        comApiLogModel.setModified(new Date());

        comApiLogDao.insert(comApiLogModel);

        OneDayRollingMySQLContext.shutdown();
    }
}