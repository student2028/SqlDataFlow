package org.student.spark.common;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.*;

public class ClassUtils {

    public static List<Class<?>> getClasses(String packageName) throws ClassNotFoundException, IOException {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        assert classLoader != null;
        String path = packageName.replace('.', '/');
        Enumeration<URL> resources = classLoader.getResources(path);
        List<File> dirs = new ArrayList<>();

        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            dirs.add(new File(resource.getFile()));
        }

        ArrayList<Class<?>> classes = new ArrayList<>();
        for (File directory : dirs) {
            classes.addAll(findClasses(directory, packageName));
        }
        return classes;
    }

    private static List<Class<?>> findClasses(File directory, String packageName) throws ClassNotFoundException {
        List<Class<?>> classes = new ArrayList<>();
        if (!directory.exists()) {
            return classes;
        }
        File[] files = directory.listFiles();
        assert files != null;
        for (File file : files) {
            if (file.isDirectory()) {
                assert !file.getName().contains(".");
                classes.addAll(findClasses(file, packageName + "." + file.getName()));
            } else if (file.getName().endsWith(".class")) {
                classes.add(Class.forName(packageName + '.' + file.getName().substring(0, file.getName().length() - 6)));
            }
        }
        return classes;
    }


    public static Map<String, Object> executeJavaFunc(String udfDefine) {
        Map<String, Object> result = new HashMap<>();
        ClassPool pool = ClassPool.getDefault();
        CtClass cc = pool.makeClass(UUID.randomUUID().toString());
        try {
            CtMethod ctMethod = CtMethod.make(udfDefine, cc);
            cc.addMethod(ctMethod);
            Object udf = cc.toClass().newInstance();
            Method udfMethod = udf.getClass().getMethod(ctMethod.getName());
            result =(Map<String,Object>) udfMethod.invoke(udf);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            cc.detach();
        }
        return result;
    }

}
