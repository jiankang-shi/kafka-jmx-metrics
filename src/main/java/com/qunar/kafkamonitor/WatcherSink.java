package com.qunar.kafkamonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WatcherSink {

    private String server;
    private int port;
    private transient GraphiteUtil graphiteUtil;

    private Logger LOGGER = LoggerFactory.getLogger(WatcherSink.class);


    public WatcherSink(String server, int port) {
        this.server = server;
        this.port = port;
    }

    public void  open() throws IOException {

        graphiteUtil = new GraphiteUtil(server, port);
        graphiteUtil.connect();
    }


    //TODO 2018-01-17 找个靠谱的graphite client
    // TODO xiaoxu.lv 有链接泄漏
    // TODO 函数不要抛异常出来了，watcher坏了，程序还是要继续工作
    // TODO 所有异常记一个指标即可

    public void invoke(final String value) throws IOException {
        try {
            graphiteUtil
                    .send(value);
            graphiteUtil.flush();
        } catch (IOException e) {
            LOGGER.error("watcher监控信息发送失败:{}",e.getMessage());
            RetryConnect(3);
            graphiteUtil
                    .send(value);
            graphiteUtil.flush();


        }
    }
    public void close() throws Exception {
        graphiteUtil.close();
    }

    private void RetryConnect(int maxTries) {
        int count = 0;
        while (true) {
            try {
                graphiteUtil.connect();
                if (graphiteUtil.isConnected()) {
                    break;
                }
            } catch (IOException e) {
                if (++count == maxTries) {
                    LOGGER.error(e.getMessage(),e);
                }
            }
        }


    }
}
