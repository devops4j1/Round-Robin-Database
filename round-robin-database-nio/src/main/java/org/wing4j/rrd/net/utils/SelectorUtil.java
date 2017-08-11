package org.wing4j.rrd.net.utils;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ConcurrentModificationException;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/8/10.
 * Nio选择器工具类
 */
public class SelectorUtil {
    static Logger LOGGER = Logger.getLogger(SelectorUtil.class.getName());

    /**
     * 重建选择器
     * @param oldSelector
     * @return
     * @throws IOException
     */
    public static Selector rebuildSelector(final Selector oldSelector) throws IOException {
        final Selector newSelector;
        try {
            newSelector = Selector.open();
        } catch (Exception e) {
            LOGGER.warning("Failed to create a new Selector." + e);
            return null;
        }

        for (; ; ) {
            try {
                for (SelectionKey key : oldSelector.keys()) {
                    Object a = key.attachment();
                    try {
                        if (!key.isValid() || key.channel().keyFor(newSelector) != null) {
                            continue;
                        }
                        int interestOps = key.interestOps();
                        key.cancel();
                        key.channel().register(newSelector, interestOps, a);
                    } catch (Exception e) {
                        LOGGER.warning("Failed to re-register a Channel to the new Selector." + e);
                    }
                }
            } catch (ConcurrentModificationException e) {
                // Probably due to concurrent modification of the key set.
                continue;
            }
            break;
        }
        oldSelector.close();
        return newSelector;
    }
}
