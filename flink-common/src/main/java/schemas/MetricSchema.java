package schemas;

import com.google.gson.Gson;
import model.Metrics;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/4
 **/
public class MetricSchema implements DeserializationSchema<Metrics>, SerializationSchema<Metrics> {
    private static Gson gson = new Gson();

    @Override
    public Metrics deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), Metrics.class);
    }

    @Override
    public boolean isEndOfStream(Metrics metrics) {
        return false;
    }

    @Override
    public byte[] serialize(Metrics metrics) {
        return gson.toJson(metrics).getBytes(Charset.forName("utf-8"));
    }

    @Override
    public TypeInformation<Metrics> getProducedType() {
        return TypeInformation.of(Metrics.class);
    }
}
