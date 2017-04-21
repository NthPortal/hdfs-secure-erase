package com.nthportal.hadoop.hdfs.erase.cli;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

final class ReflectionHelper {
    private ReflectionHelper() {}

    /**
     * Instantiates a class using its default constructor.
     *
     * @param className the name of the class
     * @return an instance of the class cast to the specified type
     * @throws CliOptionException if an exception occurs while attempting
     *                            to instantiate the class
     */
    static Object instantiate(String className) throws CliOptionException {
        try {
            Class<?> clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (ClassNotFoundException |
                NoSuchMethodException |
                IllegalAccessException |
                InstantiationException |
                InvocationTargetException e) {
            throw new CliOptionException("Exception while instantiating " + className, e);
        }
    }
}
