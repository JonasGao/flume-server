package com.x.common.flume.sink;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * {@link OneDayRollingMultiFileSink} 专用的文件路径帮助类，基于构造函数参数 {@code fileName} 拼接文件路径
 * <p>
 * 拼接用的 {@code fileName} 参数可能是上层子系统传递的项目名称（projectFile），或定时、MQ 任务的任务名称（taskName）。
 * 详细内容参见 {@link OneDayRollingMultiFileSink}
 * </p>
 * Created by jonas on 2017/4/19.
 *
 * @version 1.0
 * @see OneDayRollingMultiFileSink
 * @since 0.1.20170417162650-SNAPSHOT
 */
class OneDayRollingPathManager {

    private final String fileName;
    private final String extension;

    private File baseDirectory;
    private File currentFile;

    OneDayRollingPathManager(String fileName, File baseDirectory) {
        if (fileName.contains(".")) {
            int pointIndex = fileName.lastIndexOf(".");
            this.fileName = fileName.substring(0, pointIndex);
            this.extension = fileName.substring(pointIndex + 1);
        } else {
            this.fileName = fileName;
            this.extension = "log";
        }

        this.baseDirectory = baseDirectory;
    }

    private File nextFile() {
        StringBuilder sb = new StringBuilder();
        String todayDateTimestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE);
        sb.append(fileName).append("-").append(todayDateTimestamp);
        sb.append(".").append(extension);
        currentFile = new File(baseDirectory, sb.toString());
        return currentFile;
    }

    File getCurrentFile() {
        if (currentFile == null) {
            return nextFile();
        }

        return currentFile;
    }

    void rotate() {
        currentFile = null;
    }
}
