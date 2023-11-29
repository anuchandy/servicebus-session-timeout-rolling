package com.thrive.servicebus;

public class Config {
    public static String TOPIC = System.getenv("SERVICEBUS_TOPIC");
    public static String SUBSCRIPTION = System.getenv("SERVICEBUS_SUBSCRIPTION");
    public static String NAMESPACE = System.getenv("SERVICEBUS_NAMESPACE");
    public static String FULLY_QUALIFIED_NAMESPACE = NAMESPACE + ".servicebus.windows.net";
    public static String CONNECTION_STRING = System.getenv("SERVICEBUS_CONNECTION_STRING");
}
