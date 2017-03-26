package tr.com.serhatcan.temperature.minimum;

import tr.com.serhatcan.temperature.PlaceMainHelper;
import tr.com.serhatcan.temperature.PlaceMapper;

/**
 * Created by serhat on 26.03.2017.
 */
public class PlaceMinTemp {

    public static void main(String[] args) throws Exception {

        PlaceMainHelper.run(
                args,
                "minimum temperature",
                PlaceMinTemp.class,
                PlaceMapper.class,
                PlaceMinTempReducer.class
                );
    }
}
