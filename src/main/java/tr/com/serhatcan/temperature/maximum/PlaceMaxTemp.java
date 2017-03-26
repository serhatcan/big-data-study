package tr.com.serhatcan.temperature.maximum;

import tr.com.serhatcan.temperature.PlaceMainHelper;
import tr.com.serhatcan.temperature.PlaceMapper;
import tr.com.serhatcan.temperature.average.PlaceAvgTempReducer;

/**
 * Created by serhat on 26.03.2017.
 */
public class PlaceMaxTemp {

    public static void main(String[] args) throws Exception {

        PlaceMainHelper.run(
                args,
                "maximum temperature",
                PlaceMaxTemp.class,
                PlaceMapper.class,
                PlaceMaxTempReducer.class
                );
    }
}
