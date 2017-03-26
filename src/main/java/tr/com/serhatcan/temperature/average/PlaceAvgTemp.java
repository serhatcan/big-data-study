package tr.com.serhatcan.temperature.average;

import tr.com.serhatcan.temperature.PlaceMainHelper;
import tr.com.serhatcan.temperature.PlaceMapper;

/**
 * Created by serhat on 26.03.2017.
 */
public class PlaceAvgTemp {

    public static void main(String[] args) throws Exception {

        PlaceMainHelper.run(
                args,
                "average temperature",
                PlaceAvgTemp.class,
                PlaceMapper.class,
                PlaceAvgTempReducer.class
                );
    }
}
