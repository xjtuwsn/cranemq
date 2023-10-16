package com.github.xjtuwsn.cranemq.registry;

/**
 * @project:cranemq
 * @file:RegistryMain
 * @author:wsn
 * @create:2023/10/02-15:17
 */
public class RegistryMainStart {
    public static void main(String[] args) {
        start(build(args));
    }

    private static void start(RegistryController registryController) {
        registryController.start();
    }

    private static RegistryController build(String[] args) {
        int port = 11111;
        for (int i = 0; i < args.length; i++) {
            if ("-p".equals(args[i]) || "-P".equals(args[i])) {
                if (i == args.length - 1) {
                    break;
                }
                port = Integer.parseInt(args[i + 1]);
            }
        }
        RegistryController registryController = new RegistryController(port);

        return registryController;


    }
}
