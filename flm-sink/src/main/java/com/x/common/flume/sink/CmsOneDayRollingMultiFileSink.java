package com.x.common.flume.sink;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.RollingFileSink;

import java.io.IOException;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.defaultString;
import static org.apache.commons.lang.StringUtils.isBlank;

/**
 * 针对上层系统传递的日志，进行定制处理
 * <p>
 * 日志写入的目标，将根据传递的 Headers 中的参数 projectFile 和 taskName（以及其他多个参数，参见{@link #writeTaskFile}）
 * 来确定，并维护相关上下文。<br/>
 * 并且强制时间处理定位在格林威治时间每天的 00:00:01 进行 rolling
 * </p>
 * Created by jonas on 2017/4/18.
 * <p>
 * 该代码参考 {@link RollingFileSink} 和旧版代码中的 {@code XRollingFileSink} 修改实现
 * </p>
 */
public class CmsOneDayRollingMultiFileSink extends OneDayRollingMultiFileSink {

    private void writeTaskFile(Event event) throws IOException, EventDeliveryException {
        Map<String, String> headerMap = event.getHeaders();
        String taskName = headerMap.get("taskName");

        if (isBlank(taskName)) {
            return;
        }

        String subSystem = defaultString(headerMap.get("subSystem"));
        String splitDir = defaultString(headerMap.get("splitDir"));
        String taskLogFile = format("%s%s/%s/task-%s.log", splitDir, subSystem, taskName, taskName);

        context(taskLogFile).write(event);
    }

    private void writeProjectFile(Event event) throws EventDeliveryException, IOException {
        Map<String, String> headerMap = event.getHeaders();
        String projectFile = headerMap.get("projectFile");

        if (isBlank(projectFile)) {
            return;
        }

        context(projectFile).write(event);
    }

    @Override
    public void write(Event event) throws Exception {
        writeProjectFile(event);
        writeTaskFile(event);
    }
}
