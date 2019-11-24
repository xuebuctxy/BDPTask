package common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Administrator on 2019/11/3.
 */
public class ConfigUtil {
    private static Properties props;
    public static Properties getInstance(){
        if(props==null){
            props=new Properties();
            try {
                props.load(ConfigUtil.class.getClassLoader().getResourceAsStream("config.properties"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return props;
    }
    public static String getProperties(String key){
        return getInstance().getProperty(key);
    }
}
