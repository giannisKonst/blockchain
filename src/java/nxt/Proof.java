package nxt;

import org.json.simple.JSONObject;

public interface Proof {

    boolean verify() throws NxtException;

    JSONObject getJSONObject();

}
