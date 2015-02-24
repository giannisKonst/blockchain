package nxt;

import nxt.crypto.Crypto;
import nxt.util.Convert;
import nxt.util.Listener;
import nxt.util.Listeners;
import nxt.util.Logger;
import nxt.util.ThreadPool;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;


import java.util.SortedSet;
import nxt.BlockchainProcessor.BlockNotAcceptedException;
import nxt.BlockchainProcessor.TransactionNotAcceptedException;


public final class GeneratorPOW extends Generator {

    private volatile Listener<Block> newBlockListener;
    private volatile Block lastBlock;
    private volatile List<TransactionImpl> transactions = new ArrayList<>();
    private volatile boolean stop = true;

    //private BigInteger nonce = 0; //should need persistance
    private volatile BlockPOW block;

    public GeneratorPOW() {
    }

    public void init() {
        ThreadPool.scheduleThread("GenerateBlocks", generateBlocksTask, 500, TimeUnit.MILLISECONDS);
    }

    public void startForging(Block lastBlock) {
        if(lastBlock==null){ throw new Error(); }
        setLastBlock(lastBlock);

        stop = false;
    }

    public void pauseForging() {
    }

    public void resumeForging() {
    }

    public void onNewBlock(Listener<Block> listener) {
        this.newBlockListener = listener;
    }

    private int getValidTimestamp(){
            int timestamp = Nxt.getEpochTime();
            if(timestamp <= ((BlockPOW)lastBlock).getTimestamp()){
                timestamp = 1 + lastBlock.getTimestamp();
            }
            return timestamp;
    }

    private void renewBlock(){
        try{
            block = new BlockPOW(getValidTimestamp(), lastBlock, transactions);
        }catch(NxtException.ValidationException e){
            e.printStackTrace();
            throw new Error();
        }
    }


    synchronized public void setLastBlock(Block lastBlock) {
        this.lastBlock = lastBlock;
        renewBlock();
        Logger.logDebugMessage("setLastBlock() "+lastBlock.getJSONObject());
    }

    synchronized public void addTransaction(TransactionImpl tx) {
        transactions.add(tx);
        renewBlock();
    }
    synchronized public void setTransactions(List<TransactionImpl> txs) {
        transactions = txs;
        renewBlock();
    }

    private final Runnable generateBlocksTask = new Runnable() {

        private volatile int lastTimestamp;
        private volatile long lastBlockId;
        private final GeneratorPOW generator = GeneratorPOW.this;

        @Override
        public void run() {
            //if(true)return;
            //if(true)throw new Error("test");
            if(stop){ return; }
            //System.out.println("generateBlocksTask");
            try {
                try {
                    //Logger.logDebugMessage("LAST "+lastBlock.getJSONObject());
                    BlockPOW block = generator.block; //instead of synchronized{}
                    if(block.verifyWork()) {
                        BlockPOW.parseBlock(block.getJSONObject(), (BlockPOW)lastBlock); //TODO remove assertion
                        Logger.logDebugMessage("NEW BLOCK "+block.getJSONObject());
                        //Logger.logDebugMessage("NEW BLOCK hash="+Convert.toHexString(block.getHash()));
                        synchronized(generator) {
                            if(block == generator.block){
                                transactions = new ArrayList<>();
                                setLastBlock(block);
                            }
                        }
                        newBlockListener.notify(block); //external listener may change state
                    }else{
                        //Logger.logDebugMessage("CUR "+block.getJSONObject());
                        block.incNonce();
                        block.setTimestamp(getValidTimestamp());
                    }
                } catch (Exception e) {
                    Logger.logDebugMessage("Error in block generation thread", e);
                }
            } catch (Throwable t) {
                Logger.logMessage("CRITICAL ERROR. PLEASE REPORT TO THE DEVELOPERS.\n" + t.toString());
                t.printStackTrace();
                System.exit(1);
            }

        }

    };


}
