package utils;

import com.google.gson.Gson;

import java.nio.charset.Charset;

/**
 * Gson 序列化，反序列化操作
 **/
public class GsonUtils {
    private static final Gson gson = new Gson();

    public static <T> T fromJson(String value, Class<T> tClass) {
        return gson.fromJson(value, tClass);
    }

    public static String toJson(Object value) {
        return gson.toJson(value);
    }

    public static byte[] toJsonBytes(Object value) {
        return gson.toJson(value).getBytes(Charset.forName("utf-8"));
    }
}
