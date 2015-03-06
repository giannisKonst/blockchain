package nxt;

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

import nxt.NxtException.NotValidException;

abstract class BlockImpl implements Block {

    public static enum Init { GENESIS; }
    
    volatile int timestamp;
    final long previousBlockId;
    byte[] previousBlockHash;
    long totalAmountNQT;
    long totalFeeNQT;
    private List<TransactionImpl> blockTransactions;
    int payloadLength;
    byte[] payloadHash;

    volatile long nextBlockId;
    volatile int height = -1;
    volatile long id;
    volatile String stringId = null;

    transient final BlockImpl previousBlock;


    BlockImpl(Init init, List<TransactionImpl> transactions) throws NxtException.ValidationException {
        this(0, null, transactions);
    }

    BlockImpl(int timestamp, Block previousBlock, List<TransactionImpl> transactions) throws NxtException.ValidationException {
        if (transactions != null) {
            this.blockTransactions = Collections.unmodifiableList(transactions);
            if (blockTransactions.size() > Constants.MAX_NUMBER_OF_TRANSACTIONS) {
                throw new NxtException.NotValidException("attempted to create a block with " + blockTransactions.size() + " transactions");
            }


            /*
            for(TransactionImpl tx : transactions) {
                tx.setBlock(this); //cannot be here, because blockId is not yet finalized
            }*/
        }else{
            throw new NullPointerException("blockTransactions");
        }

        this.timestamp = timestamp;
        if (previousBlock != null){
            this.previousBlockId = previousBlock.getId();
        }else{
            this.previousBlockId = 0;
            this.previousBlockHash = new byte[32];
            this.height = 0;
        }
        this.previousBlock = (BlockImpl)previousBlock;
        calculateFields();

    }

    private void calculateFields() throws NotValidException {
            if(previousBlock != null) { //TODO
                this.previousBlockHash = previousBlock.getHash();
                this.height = previousBlock.getHeight() + 1;
            }


            long totalAmountNQT = 0;
            long totalFeeNQT = 0;
            int payloadLength = 0;
            MessageDigest digest = Crypto.sha256();

            for(Transaction tx : blockTransactions) {
                totalAmountNQT += tx.getAmountNQT();
                totalFeeNQT += tx.getFeeNQT();
                payloadLength += tx.getSize();
                digest.update(tx.getBytes());
            }
            byte[] payloadHash = digest.digest();

            this.totalAmountNQT = totalAmountNQT;
            this.totalFeeNQT = totalFeeNQT;
            this.payloadLength = payloadLength;
            this.payloadHash = payloadHash;

	    if (payloadLength > Constants.MAX_PAYLOAD_LENGTH || payloadLength < 0) {
	        throw new NxtException.NotValidException("attempted to create a block with payloadLength " + payloadLength);
	    }
    }

    BlockImpl(int timestamp, long previousBlockId, long totalAmountNQT, long totalFeeNQT, int payloadLength, byte[] payloadHash, 
              byte[] previousBlockHash, List<TransactionImpl> transactions)
            throws NxtException.ValidationException {
        //this(version, timestamp, previousBlockId, totalAmountNQT, totalFeeNQT, payloadLength, payloadHash,
          //      generatorPublicKey, generationSignature, blockSignature, previousBlockHash, null);

        this.timestamp = timestamp;
        this.previousBlockId = previousBlockId;
        this.totalAmountNQT = totalAmountNQT;
        this.totalFeeNQT = totalFeeNQT;
        this.payloadLength = payloadLength;
        this.payloadHash = payloadHash;
        this.previousBlockHash = previousBlockHash;

        this.blockTransactions = transactions;
        this.previousBlock = null; //TODO
    }

    BlockImpl(int timestamp, long previousBlockId, long totalAmountNQT, long totalFeeNQT, int payloadLength, byte[] payloadHash, 
              byte[] previousBlockHash, long nextBlockId, int height, long id)
            throws NxtException.ValidationException {
        this(timestamp, previousBlockId, totalAmountNQT, totalFeeNQT, payloadLength, payloadHash, previousBlockHash, null);

        //this.cumulativeDifficulty = cumulativeDifficulty;
        //this.baseTarget = baseTarget;
        this.nextBlockId = nextBlockId;
        this.height = height;
        this.id = id;
    }

    @Override
    public int getTimestamp() {
        return timestamp;
    }

    @Override
    public long getPreviousBlockId() {
        return previousBlockId;
    }

    @Override
    public byte[] getPreviousBlockHash() {
        return previousBlockHash;
    }

    @Override
    public long getTotalAmountNQT() {
        return totalAmountNQT;
    }

    @Override
    public long getTotalFeeNQT() {
        return totalFeeNQT;
    }

    @Override
    public int getPayloadLength() {
        return payloadLength;
    }

    @Override
    public byte[] getPayloadHash() {
        return payloadHash;
    }

    private void loadTransactions(){
            this.blockTransactions = Collections.unmodifiableList(TransactionDb.findBlockTransactions(getId()));
            for (TransactionImpl transaction : this.blockTransactions) {
                transaction.setBlock(this);
            }
    }

    @Override
    public List<TransactionImpl> getTransactions() {
        if (blockTransactions == null) {
            loadTransactions();
        }else{
            short index = 0;
            for(TransactionImpl tx : blockTransactions) {
                tx.setBlock(this); //is called here, because blockId is final at this point
                tx.setIndex(index++);
            }
        }
        return blockTransactions;
    }


    @Override
    public long getNextBlockId() {
        return nextBlockId;
    }

    @Override
    public int getHeight() {
        if (height == -1) {
            throw new IllegalStateException("Block height not yet set");
        }
        return height;
    }

    @Override
    public long getId() {
        if (id == 0) {
            byte[] hash = getHash();
            BigInteger bigInteger = new BigInteger(1, new byte[] {hash[7], hash[6], hash[5], hash[4], hash[3], hash[2], hash[1], hash[0]});
            id = bigInteger.longValue();
            stringId = bigInteger.toString();
        }
        return id;
    }

    @Override
    public String getStringId() {
        if (stringId == null) {
            getId();
            if (stringId == null) {
                stringId = Convert.toUnsignedLong(id);
            }
        }
        return stringId;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof BlockImpl && this.getId() == ((BlockImpl)o).getId();
    }

    @Override
    public int hashCode() {
        return (int)(getId() ^ (getId() >>> 32));
    }

    @Override
    public JSONObject getJSONObject() { //for Peers
        JSONObject json = new JSONObject();
        json.put("timestamp", timestamp);
        json.put("previousBlock", Convert.toUnsignedLong(previousBlockId));
        json.put("totalAmountNQT", totalAmountNQT);
        json.put("totalFeeNQT", totalFeeNQT);
        json.put("payloadLength", payloadLength);
        json.put("payloadHash", Convert.toHexString(payloadHash));
        json.put("previousBlockHash", Convert.toHexString(previousBlockHash));
        JSONArray transactionsData = new JSONArray();
        for (Transaction transaction : getTransactions()) {
            transactionsData.add(transaction.getJSONObject());
        }
        json.put("transactions", transactionsData);
        return json;
    }

//    @Override
//    abstract public JSONObject getJSONObject(boolean includeTransactions); //for http

    public BlockImpl(JSONObject blockData, BlockImpl previousVerifiedBlock) throws NxtException.ValidationException {
        try {
            this.timestamp = ((Number) blockData.get("timestamp")).intValue();
            this.previousBlockId = Convert.parseUnsignedLong((String) blockData.get("previousBlock"));
            this.previousBlockHash = Convert.parseHexString((String) blockData.get("previousBlockHash"));
            List<TransactionImpl> transactions = new ArrayList<>();
            for (Object transactionData : (JSONArray) blockData.get("transactions")) {
                transactions.add(TransactionImpl.parseTransaction((JSONObject) transactionData));
            }
            this.blockTransactions = Collections.unmodifiableList(transactions);

            if(previousVerifiedBlock == null){
                previousVerifiedBlock = BlockDb.findBlock(previousBlockId);
                if(previousVerifiedBlock == null){
                    throw new NotValidException("referenced previousBlock not found locally");
                }
            }
            previousBlock = previousVerifiedBlock;

            calculateFields();

            //TODO move elsewere
            /*
            if( !verify(previousVerifiedBlock) ) {
                throw new NotValidException("verification failed 1");
            }
            if( !verify() ) {
                throw new NotValidException("verification failed 2");
            }*/
        } catch (NxtException.ValidationException|RuntimeException e) {
            Logger.logDebugMessage("Failed to parse block: " + blockData.toJSONString());
            throw e;
        }
    }

    public int getBytesSize() {
        return ( 4 + 8 + 32 + 32 );
    }

    public byte[] getBytes() {
        //ByteBuffer buffer = ByteBuffer.allocate( 4 + 8 + 4 + (8 + 8) + 4 + 32 + 32 + 64);
        ByteBuffer buffer = ByteBuffer.allocate( getBytesSize() );
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
        System.out.println("previousBlockHash "+previousBlockHash.length);
        //buffer.putLong(nonce);
        //Logger.logDebugMessage("getBytes()= "+Convert.toHexString(buffer.array()));
        return buffer.array();
    }
    abstract public byte[] getHash();
 
    void apply() throws BlockchainProcessor.TransactionNotAcceptedException {
        for (TransactionImpl transaction : getTransactions()) {
            try {
                if (! transaction.applyUnconfirmed()) {
                    throw new BlockchainProcessor.TransactionNotAcceptedException("Double spending transaction: " + transaction.getStringId(), transaction);
                }
                transaction.apply();
            } catch (RuntimeException e) {
                Logger.logErrorMessage(e.toString(), e);
                throw new BlockchainProcessor.TransactionNotAcceptedException(e, transaction);
            }
        }
    }

    abstract boolean verify() throws NxtException.ValidationException;

    boolean verify(Block previousBlock) {
        if (previousBlock.getId() != this.getPreviousBlockId()) {
            System.out.println(previousBlock.getId()+"!="+this.getPreviousBlockId());
            return false;
            //throw new NxtException.NotValidException("Previous block id doesn't match");
        }

        return true;
    }

    abstract public boolean betterThan(Block chainHead);

    void setPrevious(Block block) {
        if (block != null) {
            if (block.getId() != getPreviousBlockId()) {
                // shouldn't happen as previous id is already verified, but just in case //TODO where?
                throw new IllegalStateException("Previous block id doesn't match");
            }
            this.height = block.getHeight() + 1;
        } else {
            this.height = 0;
        }
        /* //TODO why is that here?
        short index = 0;
        for (TransactionImpl transaction : getTransactions()) {
            transaction.setBlock(this);
            transaction.setIndex(index++);
        }*/
    }

    abstract public BigInteger getCumulativeDifficulty();
    //abstract public void persist();
}
