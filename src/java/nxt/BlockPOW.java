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
    //private BlockPOW previousBlock;


    BlockPOW(int timestamp, Block previousBlock_, List<TransactionImpl> transactions) throws NxtException.ValidationException {
	super(timestamp, previousBlock_, transactions);

        //this.previousBlock = (BlockPOW)previousBlock_;
        this.calculateBaseTarget((BlockPOW)previousBlock);
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

    public int getBytesSize() {
        return 8 + super.getBytesSize();
    }

    public byte[] getBytes() {
        byte[] bytes = super.getBytes();
        int offset = super.getBytesSize();
        ByteBuffer buffer = ByteBuffer.wrap( bytes, offset, bytes.length-offset );
        buffer.putLong(nonce);
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
            final int calibrationInterval = 201;
            if( getHeight() % calibrationInterval != 0){
                baseTarget = previousBlock.baseTarget;
            }else{ //calibrate difficulty
                Blockchain chain = BlockchainImpl.getInstance();
                synchronized (chain) {
                    Block prevCalibration;
                    if(chain.getLastBlock().getId() == previousBlockId || chain.hasBlock(previousBlockId) ) {
                        prevCalibration = chain.getBlockAtHeight( this.getHeight() - calibrationInterval );
                    } else {
                        BlockPOW block = previousBlock;
                        while(block.height > this.height - calibrationInterval) {
                            block = (BlockPOW)block.previousBlock;
                            if(block == null){ //TODO when?
                                throw new Error("develop");
                            }
                        }
                        prevCalibration = block;
                    }
                    int timeDiff = this.timestamp - prevCalibration.getTimestamp();
                    int wantedBlockInterval = 15; //seconds
                    int wantedTimeDiff = wantedBlockInterval * calibrationInterval;
                    baseTarget = previousBlock.baseTarget * timeDiff/wantedTimeDiff;
                    if(baseTarget < 0){
                         Logger.logDebugMessage("calculateBaseTarget() OVERFLOW");
                        //throw new Error("target overflow. choose other type?");
                         baseTarget = Long.MAX_VALUE;
                    }
                    Logger.logDebugMessage("calculateBaseTarget() prev="+previousBlock.baseTarget+" cur="+baseTarget);
                }
                Logger.logDebugMessage("adjustBaseTarget() "+baseTarget);
            }

            cumulativeDifficulty = previousBlock.cumulativeDifficulty.add(Convert.two64.divide(BigInteger.valueOf(baseTarget)));
    }

    boolean verify() throws NxtException.ValidationException {
        return this.verifyWork();
    }
 
    boolean verifyWork() {
        byte[] h = this.getHash();
        byte[] lowHash = {h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]};
        BigInteger hit = new BigInteger(1, lowHash); //TODO why only the low part?
        BigInteger target = BigInteger.valueOf(this.baseTarget);
        //Logger.logDebugMessage("hit   ="+hit);
        //Logger.logDebugMessage("target="+target);
        return  hit.compareTo(target) == -1;
    }

    boolean verify(Block previousBlock) {
        if(this.timestamp <= ((BlockPOW)previousBlock).timestamp){
          return false;
        }
        return super.verify(previousBlock);
    }

    BlockPOW(Init genesis, List<TransactionImpl> txs) throws NxtException.ValidationException { //GENESIS BLOCK
        super(genesis, txs);
        baseTarget = Long.MAX_VALUE /1000;
        baseTarget = 1468397538205671l;
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

            return new BlockPOW(Init.GENESIS, transactions);
        }catch(Exception e){
            e.printStackTrace();
            throw new Error("GenesisBlock");
        }
    }

   public BlockPOW(JSONObject blockData, BlockImpl previousVerifiedBlock) throws NxtException.ValidationException {
        super(blockData, previousVerifiedBlock);

        long nonce = (Long) blockData.get("nonce");
        this.nonce = nonce;

        calculateBaseTarget((BlockPOW)previousBlock);
    }

    //TODO remove
    public int getVersion(){throw new Error("develop");}
    public  byte[] getBlockSignature(){throw new Error("develop");}
    public long getGeneratorId() { throw new Error("develop"); }
    public byte[] getGenerationSignature(){throw new Error("develop");}
    public byte[] getGeneratorPublicKey(){throw new Error("develop");}

}
