package com.xuehui.conf;

import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 *
 *
 */
@Component
public class ConfigurationManager {
    private static Properties pro = new Properties();

    static {
        try {
            InputStream is = ConfigurationManager.class.getClassLoader().getResourceAsStream("jdbc.properties");
            pro.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static String getProperty(String key){
        return pro.getProperty(key);
    }

    public static Integer getInteger(String key){
        try {
            String value = pro.getProperty(key);
            return Integer.valueOf(value);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static Boolean getBoolean(String key){
        try {
            String value = pro.getProperty(key);
            return Boolean.valueOf(value);
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
}
