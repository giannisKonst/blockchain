package nxt.http;

import nxt.GeneratorNXT;
import org.json.simple.JSONObject;
import org.json.simple.JSONStreamAware;

import javax.servlet.http.HttpServletRequest;

import static nxt.http.JSONResponses.MISSING_SECRET_PHRASE;
import static nxt.http.JSONResponses.UNKNOWN_ACCOUNT;

import static nxt.http.JSONResponses.MISSING_NAME;

import nxt.Generator;
import nxt.GeneratorPOW;
import nxt.GeneratorAgreement;


public final class StartForging extends APIServlet.APIRequestHandler {

    static final StartForging instance = new StartForging();

    private StartForging() {
        super(new APITag[] {APITag.FORGING}, "input", "secretPhrase");
    }

    @Override
    JSONStreamAware processRequest(HttpServletRequest req) {

        String input = req.getParameter("input");
        if (input == null) {
            return MISSING_NAME;
        }
        String actor = req.getParameter("secretPhrase");
        if (actor == null) {
            return MISSING_SECRET_PHRASE;
        }

        GeneratorAgreement generator = (GeneratorAgreement)Generator.getInstance();
        generator.setInput(input);
        generator.setActor(actor);
        if (!generator.isForging()) {
            generator.startForging();
        }

        JSONObject response = new JSONObject();
        //response.put("deadline", generator.getDeadline());
        //response.put("hitTime", generator.getHitTime());
        response.put("forging", true);
        return response;

    }

    @Override
    boolean requirePost() {
        return true;
    }

}
