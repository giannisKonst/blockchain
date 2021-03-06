package nxt;

import org.json.simple.JSONObject;

public class ProofOfWork implements Proof {

	private long nonce;
	private Block block;

	private ProofOfWork() {};

	public ProofOfWork(Block block) {
		this.block = block;
	}


	public boolean verify() throws NxtException {

		//long baseTarget = block.getBaseTarget();

		//return block.getHash() < baseTarget;
		return true;
	}

	public JSONObject getJSONObject(){
		JSONObject json = new JSONObject();
		json.put("nonce", nonce);
		return json;
	}

}
