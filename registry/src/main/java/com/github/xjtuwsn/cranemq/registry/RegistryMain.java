package com.github.xjtuwsn.cranemq.registry;

import com.github.xjtuwsn.cranemq.registry.core.Registry;

/**
 * @project:cranemq
 * @file:RegistryMain
 * @author:wsn
 * @create:2023/10/02-15:17
 */
public class RegistryMain {
    public static void main(String[] args) {
        Registry registry = new Registry();
        registry.start();
    }
}
