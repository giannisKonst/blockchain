package nxt;

import org.json.simple.JSONObject;

import nxt.crypto.Crypto;
import nxt.util.Convert;
import nxt.util.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import nxt.Transaction;
import nxt.TransactionImpl;
import java.util.Comparator;

import nxt.NxtException.NotValidException;

public class BlockBA extends BlockPOW {
    private final String vote;
    private final String actorsDiscriminator; //prevent collisions for honest actors

    BlockBA(Init init, List<TransactionImpl> transactions) throws NxtException.ValidationException {
        super(init, transactions);
        vote = "_GENESIS_";
        actorsDiscriminator = vote;
    }

    public static BlockBA getGenesisBlock() {
        try{
            return new BlockBA(Init.GENESIS, new ArrayList<TransactionImpl>());
        }catch(Exception e){
            throw new Error("genesis block", e);
        }
    }

    BlockBA(String vote, String actorsDiscriminator, int timestamp, Block previousBlock_, List<TransactionImpl> transactions) throws NxtException.ValidationException {
	super(timestamp, previousBlock_, transactions);

        if(vote == null) {
            throw new NullPointerException("block must have a vote"); }
        if(actorsDiscriminator == null) {
            throw new NullPointerException("block must have an actor specific value"); }

        this.vote = vote;
        this.actorsDiscriminator = actorsDiscriminator;
    }

    BlockBA(int timestamp, long previousBlockId, long totalAmountNQT, long totalFeeNQT, int payloadLength, byte[] payloadHash, 
              byte[] previousBlockHash, long nextBlockId, int height, long id, long nonce, BigInteger cumulativeDifficulty, long baseTarget,
              String vote, String actorsDiscriminator)
              throws NxtException.ValidationException {
	super(timestamp, previousBlockId, totalAmountNQT, totalFeeNQT, payloadLength, payloadHash, previousBlockHash,
                    nextBlockId, height, id, nonce, cumulativeDifficulty, baseTarget);
        this.vote = vote;
        this.actorsDiscriminator = actorsDiscriminator;

   }

    public BlockBA(JSONObject blockData, BlockImpl previousVerifiedBlock) throws NxtException.ValidationException {
        super(blockData, previousVerifiedBlock);
        //TODO impose some restraints on input?
        vote = (String) blockData.get("vote");
        actorsDiscriminator = (String) blockData.get("actor");
    }
 
   public int getBytesSize() {
        return 32 + super.getBytesSize();
    }

    public byte[] getBytes() {
        byte[] bytes = super.getBytes();
        int offset = super.getBytesSize();
        ByteBuffer buffer = ByteBuffer.wrap( bytes, offset, bytes.length-offset );
        MessageDigest digest = Crypto.sha256();
	digest.update(this.vote.getBytes()); //TODO String.getBytes()
	digest.update(this.actorsDiscriminator.getBytes()); //TODO String.getBytes()
        buffer.put(digest.digest());
        return buffer.array();
    }

    @Override
    public JSONObject getJSONObject() { //for Peers
        JSONObject json = super.getJSONObject();
        json.put("vote", vote);
        json.put("actor", actorsDiscriminator);
        return json;
    }

    public String getVote() {
        return vote;
    }
    public String getActor() {
        return actorsDiscriminator;
    }

}
