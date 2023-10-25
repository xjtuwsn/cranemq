package com.github.xjtuwsn.cranemq.broker.store.flush;

import com.github.xjtuwsn.cranemq.broker.store.MappedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @project:cranemq
 * @file:SyncFlushDiskService
 * @author:wsn
 * @create:2023/10/06-11:01
 * 同步刷盘服务
 */
public class SyncFlushDiskService implements FlushDiskService {
    private static final Logger log = LoggerFactory.getLogger(SyncFlushDiskService.class);
    @Override
    public void flush() {

    }

    @Override
    public void flush(MappedFile mappedFile) {
        if (mappedFile == null) {
            log.error("Flush disg get null mappedfile");
            return;
        }
        mappedFile.doFlush();
    }
}
