package com.github.xjtuwsn.cranemq.common.remote;

import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;

/**
 * @project:cranemq
 * @file:RegistryCallback
 * @author:wsn
 * @create:2023/10/15-15:43
 */
public interface RegistryCallback {

    void onRouteInfo(TopicRouteInfo info);
}
