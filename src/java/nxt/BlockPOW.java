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

public class BlockPOW extends BlockImpl {

    private BigInteger nonce = BigInteger.ZERO;
    private int timestamp;
    private BigInteger cumulativeDifficulty = BigInteger.ZERO;
    private long baseTarget = Constants.INITIAL_BASE_TARGET;


    BlockPOW(int timestamp, BlockPOW previousBlock, List<Transaction> transactions) {
	super(timestamp, previousBlock, transactions);
    }

    @Override
    public byte[] getHash() {
        return Crypto.sha256().digest(this.getBytes());
    }

    public long getBaseTarget() {
        return baseTarget;
    }

    public BigInteger getCumulativeDifficulty() {
        return cumulativeDifficulty;
    }

    public void incNonce() {
        nonce = nonce.add(BigInteger.ONE);
    }

	/*
    @Override
    public JSONObject getJSONObject() { //for Peers
        JSONObject json = new JSONObject();
        json.put("version", version);
        json.put("timestamp", timestamp);
        json.put("previousBlock", Convert.toUnsignedLong(previousBlockId));
        json.put("totalAmountNQT", totalAmountNQT);
        json.put("totalFeeNQT", totalFeeNQT);
        json.put("payloadLength", payloadLength);
        json.put("payloadHash", Convert.toHexString(payloadHash));
        json.put("generatorPublicKey", Convert.toHexString(generatorPublicKey));
        json.put("generationSignature", Convert.toHexString(generationSignature));
        if (version > 1) {
            json.put("previousBlockHash", Convert.toHexString(previousBlockHash));
        }
        json.put("blockSignature", Convert.toHexString(blockSignature));
        JSONArray transactionsData = new JSONArray();
        for (Transaction transaction : getTransactions()) {
            transactionsData.add(transaction.getJSONObject());
        }
        json.put("transactions", transactionsData);
        return json;
    }*/

    /*
    public JSONObject getJSONObject() {
        this.getJSONObject(false);
    }*/

    public byte[] getBytes() {
        ByteBuffer buffer = ByteBuffer.allocate( 4 + 8 + 4 + (8 + 8) + 4 + 32 );
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(timestamp);
        buffer.putLong(previousBlockId);
        buffer.putInt(getTransactions().size());
        buffer.putLong(totalAmountNQT);
        buffer.putLong(totalFeeNQT);
        buffer.putInt(payloadLength);
        buffer.put(payloadHash);
        buffer.put(previousBlockHash);
        return buffer.array();
    }

    @Override
    void setPrevious(Block block1) {
        BlockPOW block = (BlockPOW) block1;
        if (block != null) {
            this.calculateBaseTarget(block);
        }
    }

    private void calculateBaseTarget(BlockPOW previousBlock) {
        //if (this.getId() != Genesis.GENESIS_BLOCK_ID || previousBlockId != 0) {
            if( getHeight() % 2012 != 0){
                baseTarget = previousBlock.baseTarget;
            }else{
                //baseTarget = average of last 2012 blocks; //TODO
                baseTarget = previousBlock.baseTarget;
            }
            cumulativeDifficulty = previousBlock.cumulativeDifficulty.add(Convert.two64.divide(BigInteger.valueOf(baseTarget)));
        //}
    }

    boolean verify() {
        return this.verifyWork();
    }
 
    boolean verifyWork() {
        return this.getHash() < baseTarget;
    }

}
