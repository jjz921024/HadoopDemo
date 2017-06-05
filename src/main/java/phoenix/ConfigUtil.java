package phoenix;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Jun on 2017/6/1.
 */
public class ConfigUtil {
    private static Properties p = new Properties();

    static {
        try {
            p.load(ClassLoader.getSystemResourceAsStream("phoenix.properties"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getDriver() {
        return p.getProperty("phoenix.driver");
    }

    public static String getUrl() {
        return p.getProperty("phoenix.url");
    }

    public static String getUsername() {
        return p.getProperty("phoenix.username");
    }

    public static String getPassword() {
        return p.getProperty("phoenix.password");
    }


}
