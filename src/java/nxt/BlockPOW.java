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

public class BlockPOW extends BlockImpl {

    //private BigInteger nonce = BigInteger.ZERO;
    private long nonce = 0; //TODO max nonce=?
    //private int timestamp = Nxt.getEpochTime();
    private BigInteger cumulativeDifficulty = BigInteger.ZERO;
    private long baseTarget;


    BlockPOW(int timestamp, Block previousBlock_, List<TransactionImpl> transactions) throws NxtException.ValidationException {
	super(timestamp, previousBlock_, transactions);

        BlockPOW previousBlock = (BlockPOW)previousBlock_;
        this.calculateBaseTarget(previousBlock);
        this.timestamp = timestamp;
        if(this.timestamp <= previousBlock.timestamp){ //TODO should not check -- fix the callers properly
          throw new RuntimeException("wrong timestamp");
          //this.timestamp = 1+previousBlock.timestamp;
        }
    }

    BlockPOW(int timestamp, long previousBlockId, long totalAmountNQT, long totalFeeNQT, int payloadLength, byte[] payloadHash, 
              byte[] previousBlockHash, long nextBlockId, int height, long id, long nonce, BigInteger cumulativeDifficulty, long baseTarget)
              throws NxtException.ValidationException {
       super(timestamp, previousBlockId, totalAmountNQT, totalFeeNQT, payloadLength, payloadHash, 
               previousBlockHash, nextBlockId, height, id);
       this.nonce = nonce;
       this.cumulativeDifficulty = cumulativeDifficulty;
       this.baseTarget = baseTarget;
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

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public long getNonce() {
        return nonce;
    }

    public void incNonce() {
        //nonce = nonce.add(BigInteger.ONE);
        nonce++;
    }


    @Override
    public JSONObject getJSONObject() { //for Peers
        JSONObject json = super.getJSONObject();
        json.put("nonce", nonce);

        //FOR DEBUG ONLY
        json.put("baseTarget", baseTarget);
        json.put("height", height);
        return json;
    }

    /*
    public JSONObject getJSONObject() {
        this.getJSONObject(false);
    }*/

    public byte[] getBytes() {
        //ByteBuffer buffer = ByteBuffer.allocate( 4 + 8 + 4 + (8 + 8) + 4 + 32 + 32 + 64);
        ByteBuffer buffer = ByteBuffer.allocate( 4 + 8 + 32 + 32 + 8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        //Logger.logDebugMessage("getBytes() timestamp="+timestamp);
        buffer.putInt(timestamp);
        buffer.putLong(previousBlockId);
        //buffer.putInt(getTransactions().size());
        //buffer.putLong(totalAmountNQT);
        //buffer.putLong(totalFeeNQT);
        //buffer.putInt(payloadLength);
        buffer.put(payloadHash);
        buffer.put(previousBlockHash);
        buffer.putLong(nonce);
        //Logger.logDebugMessage("getBytes()= "+Convert.toHexString(buffer.array()));
        return buffer.array();
    }

    /*
    @Override
    void setPrevious(Block block1) {
        super.setPrevious(block1);
        BlockPOW block = (BlockPOW) block1;
        if (block != null) {
            this.calculateBaseTarget(block);
        }
    }*/

    public boolean betterThan(Block block1){
        BlockPOW block = (BlockPOW) block1;
        Logger.logDebugMessage("betterThan() "+this.cumulativeDifficulty+" to "+block.cumulativeDifficulty);
        return cumulativeDifficulty.compareTo( block.cumulativeDifficulty ) == 1;
    }

    private void calculateBaseTarget(BlockPOW previousBlock) {
        //if (this.getId() != Genesis.GENESIS_BLOCK_ID || previousBlockId != 0) {
            if( getHeight() % 2012 != 0){
                baseTarget = previousBlock.baseTarget;
            }else{
                baseTarget = previousBlock.baseTarget;
                //baseTarget = average of last 2012 blocks; //TODO
                Blockchain chain = BlockchainImpl.getInstance();
                synchronized (chain) {
                    if(chain.getLastBlock().equals(this)) { 
                    }else{
                    }
                }
                Logger.logDebugMessage("adjustBaseTarget() "+baseTarget);
            }
            cumulativeDifficulty = previousBlock.cumulativeDifficulty.add(Convert.two64.divide(BigInteger.valueOf(baseTarget)));
        //}
    }

    boolean verify() {
        return this.verifyWork();
    }
 
    boolean verifyWork() {
        byte[] h = this.getHash();
        byte[] lowHash = {h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]};
        BigInteger baseTarget = BigInteger.valueOf(this.baseTarget);
        BigInteger hit = new BigInteger(1, lowHash);
        //System.out.println("hit   ="+hit);
        //System.out.println("target="+baseTarget);
        //Logger.logDebugMessage("hit       ="+hit);
        //Logger.logDebugMessage("baseTarget="+baseTarget);
        return  hit.compareTo(baseTarget) == -1;
    }

    boolean verify(Block previousBlock) {
        if(this.timestamp <= ((BlockPOW)previousBlock).timestamp){
          return false;
        }
        return super.verify(previousBlock);
    }

    private BlockPOW(List<TransactionImpl> txs) throws Exception { //GENESIS BLOCK
        super(0, null, txs);
        baseTarget = Long.MAX_VALUE /10;
    }

    public static Block getGenesisBlock() {
        try {
	    List<TransactionImpl> transactions = new ArrayList<>();
	    for (int i = 0; i < Genesis.GENESIS_RECIPIENTS.length; i++) {
		TransactionImpl transaction = new TransactionImpl.BuilderImpl((byte) 0, Genesis.CREATOR_PUBLIC_KEY,
			Genesis.GENESIS_AMOUNTS[i] * Constants.ONE_NXT, 0, (short) 0,
			Attachment.ORDINARY_PAYMENT)
			.timestamp(0)
			.recipientId(Genesis.GENESIS_RECIPIENTS[i])
			.height(0)
			.ecBlockHeight(0)
			.ecBlockId(0)
			.build();
                transaction.sign(Genesis.secret);
		transactions.add(transaction);
	    }

            return new BlockPOW(transactions);
        }catch(Exception e){
            e.printStackTrace();
            throw new Error("GenesisBlock");
        }
    }

   static BlockPOW parseBlock(JSONObject blockData, BlockImpl previousVerifiedBlock) throws NxtException.ValidationException {
        try {
            int timestamp = ((Number) blockData.get("timestamp")).intValue();
            long previousBlockId = Convert.parseUnsignedLong((String) blockData.get("previousBlock"));
            byte[] previousBlockHash = Convert.parseHexString((String) blockData.get("previousBlockHash"));
            List<TransactionImpl> blockTransactions = new ArrayList<>();
            for (Object transactionData : (JSONArray) blockData.get("transactions")) {
                blockTransactions.add(TransactionImpl.parseTransaction((JSONObject) transactionData));
            }

            long nonce = (Long) blockData.get("nonce");

            if(previousVerifiedBlock == null){
                previousVerifiedBlock = BlockDb.findBlock(previousBlockId);
            }
            BlockPOW block = new BlockPOW(timestamp, previousVerifiedBlock, blockTransactions);
            block.nonce = nonce;

            if( !block.verify(previousVerifiedBlock) ) {
                throw new NotValidException("verification failed 1");
            }
            if( !block.verify() ) {
                throw new NotValidException("verification failed 2");
            }
            return block;
        } catch (NxtException.ValidationException|RuntimeException e) {
            Logger.logDebugMessage("Failed to parse block: " + blockData.toJSONString());
            throw e;
        }
    }

    //TODO remove
    public int getVersion(){throw new Error("develop");}
    public  byte[] getBlockSignature(){throw new Error("develop");}
    public long getGeneratorId() { throw new Error("develop"); }
    public byte[] getGenerationSignature(){throw new Error("develop");}
    public byte[] getGeneratorPublicKey(){throw new Error("develop");}

}
