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

abstract class BlockImpl implements Block {

    
    final int timestamp;
    final long previousBlockId;
    final byte[] previousBlockHash;
    final long totalAmountNQT;
    final long totalFeeNQT;
    volatile List<TransactionImpl> blockTransactions;
    final int payloadLength;
    final byte[] payloadHash;

    volatile long nextBlockId;
    int height = -1;
    volatile long id;
    volatile String stringId = null;


    //Class childClass = BlockNXTImpl.class;

    BlockImpl(int timestamp, Block previousBlock, List<Transaction> transactions) throws NxtException.ValidationException {


        if (payloadLength > Constants.MAX_PAYLOAD_LENGTH || payloadLength < 0) {
            throw new NxtException.NotValidException("attempted to create a block with payloadLength " + payloadLength);
        }



        this.timestamp = timestamp;
        this.previousBlockId = previousBlock.getId();
        //this.generatorPublicKey = generatorPublicKey;
        //this.generationSignature = generationSignature;
        //this.blockSignature = blockSignature;
        this.previousBlockHash = previousBlock.getHash();

        if (transactions != null) {
            this.blockTransactions = Collections.unmodifiableList(transactions);
            if (blockTransactions.size() > Constants.MAX_NUMBER_OF_TRANSACTIONS) {
                throw new NxtException.NotValidException("attempted to create a block with " + blockTransactions.size() + " transactions");
            }
    
            long totalAmountNQT = 0;
            long totalFeeNQT = 0;
            int payloadLength = 0;
            MessageDigest digest = Crypto.sha256();

            for(Transaction tx : transactions) {
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
        }

    }

	/*
    BlockImpl(int version, int timestamp, long previousBlockId, long totalAmountNQT, long totalFeeNQT, int payloadLength,
              byte[] payloadHash, byte[] generatorPublicKey, byte[] generationSignature, byte[] blockSignature,
              byte[] previousBlockHash, BigInteger cumulativeDifficulty, long baseTarget, long nextBlockId, int height, long id)
            throws NxtException.ValidationException {
        this(version, timestamp, previousBlockId, totalAmountNQT, totalFeeNQT, payloadLength, payloadHash,
                generatorPublicKey, generationSignature, blockSignature, previousBlockHash, null);
        this.cumulativeDifficulty = cumulativeDifficulty;
        this.baseTarget = baseTarget;
        this.nextBlockId = nextBlockId;
        this.height = height;
        this.id = id;
    }*/


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

    @Override
    public List<TransactionImpl> getTransactions() {
        if (blockTransactions == null) {
            this.blockTransactions = Collections.unmodifiableList(TransactionDb.findBlockTransactions(getId()));
            for (TransactionImpl transaction : this.blockTransactions) {
                transaction.setBlock(this);
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
    abstract public long getId();

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
    abstract public JSONObject getJSONObject(); //for Peers

    @Override
    abstract public JSONObject getJSONObject(boolean includeTransactions); //for http

    static BlockImpl parseBlock(JSONObject blockData) throws NxtException.ValidationException {
	return BlockNXTImpl.parseBlock(blockData); //TODO
    }

    abstract public byte[] getBytes();
    abstract public byte[] getHash();
 
    void apply() throws BlockchainProcessor.TransactionNotAcceptedException {
        for (TransactionImpl transaction : getTransactions()) {
            try {
                transaction.apply();
            } catch (RuntimeException e) {
                Logger.logErrorMessage(e.toString(), e);
                throw new BlockchainProcessor.TransactionNotAcceptedException(e, transaction);
            }
        }
    }

    abstract boolean verify();

    void setPrevious(BlockImpl block) {
        if (block != null) {
            if (block.getId() != getPreviousBlockId()) {
                // shouldn't happen as previous id is already verified, but just in case
                throw new IllegalStateException("Previous block id doesn't match");
            }
            this.height = block.getHeight() + 1;
        } else {
            this.height = 0;
        }
        short index = 0;
        for (TransactionImpl transaction : getTransactions()) {
            transaction.setBlock(this);
            transaction.setIndex(index++);
        }
    }

    abstract public BigInteger getCumulativeDifficulty();
    //abstract public void persist();
}
