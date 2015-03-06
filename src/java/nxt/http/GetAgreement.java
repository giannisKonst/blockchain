package nxt.http;

import nxt.GeneratorNXT;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.JSONStreamAware;

import javax.servlet.http.HttpServletRequest;


import nxt.Generator;
import nxt.GeneratorPOW;
import nxt.GeneratorAgreement;

import nxt.Db;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class GetAgreement extends APIServlet.APIRequestHandler {

    static final GetAgreement instance = new GetAgreement();

    private GetAgreement() {
        super(new APITag[] {APITag.FORGING}, "input");
    }

    @Override
    JSONStreamAware processRequest(HttpServletRequest req) {

        JSONArray candidates = new JSONArray();

        try (Connection con = Db.db.getConnection();
             PreparedStatement pstmt = con.prepareStatement("SELECT vote, count(*) as count FROM block WHERE height*2 <= (select max(height) from block) GROUP BY vote ORDER BY count DESC")) {
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
		    JSONObject candidate = new JSONObject();
		    candidate.put("name", rs.getString("vote"));
		    candidate.put("votes", rs.getInt("count"));
		    candidates.add(candidate);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e.toString(), e);
        }


        GeneratorAgreement generator = (GeneratorAgreement)Generator.getInstance();
        String input = generator.getInput();
        boolean forging = generator.isForging();



        JSONObject response = new JSONObject();
        response.put("candidates", candidates);
        response.put("input", input);
        response.put("forging", forging);
        return response;

    }

}
