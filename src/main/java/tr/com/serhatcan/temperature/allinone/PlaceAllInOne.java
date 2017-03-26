package tr.com.serhatcan.temperature.allinone;

import tr.com.serhatcan.temperature.PlaceMainHelper;
import tr.com.serhatcan.temperature.PlaceMapper;

/**
 * Created by serhat on 26.03.2017.
 */
public class PlaceAllInOne {

    public static void main(String[] args) throws Exception {

        PlaceMainHelper.run(
                args,
                "temperature all in one",
                PlaceAllInOne.class,
                PlaceMapper.class,
                PlaceAllInOneReducer.class
                );
    }
}
