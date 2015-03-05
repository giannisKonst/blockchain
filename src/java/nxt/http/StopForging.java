package nxt.http;

import nxt.GeneratorNXT;
import org.json.simple.JSONObject;
import org.json.simple.JSONStreamAware;

import javax.servlet.http.HttpServletRequest;

//import static nxt.http.JSONResponses.MISSING_SECRET_PHRASE;
import nxt.Generator;

public final class StopForging extends APIServlet.APIRequestHandler {

    static final StopForging instance = new StopForging();

    private StopForging() {
        super(new APITag[] {APITag.FORGING} );
    }

    @Override
    JSONStreamAware processRequest(HttpServletRequest req) {

        Generator.getInstance().stopForging();

        JSONObject response = new JSONObject();
        response.put("stopped", true);
        return response;

    }

    @Override
    boolean requirePost() {
        return true;
    }

}
