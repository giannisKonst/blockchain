package nxt;

import java.math.BigInteger;
import java.util.Arrays;
import nxt.util.Logger;

import nxt.crypto.Crypto;
import java.security.MessageDigest;
import org.json.simple.JSONObject;

/*
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
*/

public class ProofOfNXT /*implements Proof*/ {

	long proof;
	BlockNXT block;

	private ProofOfNXT() {};

/*
	public ProofOfNXT(BlockNXT block) {
		this.block = block;
	}


	public boolean verify() throws NxtException {
		return verifyGenerationSignature();
	}
*/	

    static public boolean verifyHit(BigInteger hit, BigInteger effectiveBalance, Block previousBlock1, int timestamp) {
	BlockNXT previousBlock = (BlockNXT)previousBlock1;
        int elapsedTime = timestamp - previousBlock.getTimestamp();
        if (elapsedTime <= 0) {
            return false;
        }
        BigInteger effectiveBaseTarget = BigInteger.valueOf(previousBlock.getBaseTarget()).multiply(effectiveBalance);
        BigInteger prevTarget = effectiveBaseTarget.multiply(BigInteger.valueOf(elapsedTime - 1));
        BigInteger target = prevTarget.add(effectiveBaseTarget);
        return hit.compareTo(target) < 0
                && (previousBlock.getHeight() < Constants.TRANSPARENT_FORGING_BLOCK_8
                || hit.compareTo(prevTarget) >= 0
                || (Constants.isTestnet ? elapsedTime > 300 : elapsedTime > 3600)
                || Constants.isOffline);
    }

	public JSONObject getJSONObject(){
		JSONObject json = new JSONObject();
		json.put("proof", proof);
		return json;
	}

    static BigInteger getHit(byte[] publicKey, Block block1) {
	BlockNXT block = (BlockNXT) block1;
	/*if (allowsFakeForging(publicKey)) {
            return BigInteger.ZERO;
        }*/
        if (block.getHeight() < Constants.TRANSPARENT_FORGING_BLOCK) {
            throw new IllegalArgumentException("Not supported below Transparent Forging Block");
        }
        MessageDigest digest = Crypto.sha256();
        digest.update(block.getGenerationSignature());
        byte[] generationSignatureHash = digest.digest(publicKey);
        return new BigInteger(1, new byte[] {generationSignatureHash[7], generationSignatureHash[6], generationSignatureHash[5], generationSignatureHash[4], generationSignatureHash[3], generationSignatureHash[2], generationSignatureHash[1], generationSignatureHash[0]});
    }

    static long getHitTime(Account account, Block block1) {
	BlockNXT block = (BlockNXT) block1;
        return getHitTime(BigInteger.valueOf(account.getEffectiveBalanceNXT()), getHit(account.getPublicKey(), block), block);
    }

    static long getHitTime(BigInteger effectiveBalance, BigInteger hit, Block block1) {
	BlockNXT block = (BlockNXT) block1;
        return block.getTimestamp()
                + hit.divide(BigInteger.valueOf(block.getBaseTarget()).multiply(effectiveBalance)).longValue();
    }

}
