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


public class GeneratorPOW extends Generator {

    private volatile Listener<Block> newBlockListener;
    private volatile Block lastBlock;
    private volatile List<TransactionImpl> transactions = new ArrayList<>();

    private volatile boolean stop = true;
    //private final Object lock = new Object();

    //private BigInteger nonce = 0; //should need persistance
    private volatile BlockPOW block;

    public GeneratorPOW() {
    }

    public void init() {
        ThreadPool.scheduleThread("GenerateBlocks", generateBlocksTask, 1, TimeUnit.MILLISECONDS);
    }

    synchronized public boolean isForging() {
        return !stop;
    }

    synchronized public void stopForging() {
        stop = true;
    }

    synchronized public void startForging() {
        if(getBlock() == null) {
            throw new IllegalStateException("Generator is not yet fully initialized");
        }
        stop = false;
        //synchronized(lock){ lock.notifyAll(); }
    }

    public void onNewBlock(Listener<Block> listener) {
        this.newBlockListener = listener;
    }

    synchronized int getValidTimestamp() {
            int timestamp = Nxt.getEpochTime();
            if(timestamp <= ((BlockPOW)lastBlock).getTimestamp()){
                timestamp = 1 + lastBlock.getTimestamp();
            }
            return timestamp;
    }

    synchronized BlockPOW getBlock() {
        return block;
    }

    synchronized void renewBlock() {
        try{
            block = new BlockPOW(getValidTimestamp(), lastBlock, transactions);
        }catch(NxtException.ValidationException e){
            e.printStackTrace();
            throw new Error();
        }
    }

    synchronized public Block getLastBlock() {
        return lastBlock;
    }

    synchronized public void setLastBlock(Block lastBlock) {
        if(lastBlock == null){ throw new Error(); }
        this.lastBlock = lastBlock;
        renewBlock();
        //Logger.logDebugMessage("setLastBlock() "+lastBlock.getJSONObject());
    }

    synchronized public List<TransactionImpl> getTransactions() {
        return transactions;
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
        private final int batchSize = 2;

        @Override
        public void run() {
            //System.out.println("generateBlocksTask");
            if(stop) {
                return; /*
                synchronized(lock) {  //TODO
                    if(stop){ try {
                        lock.wait();
                    }catch(InterruptedException e){}}
                }*/
            }
            for(int i=0; i < batchSize; i++) {
                work();
            }
        }

        private void work() {
            try {
                try {
                    //Logger.logDebugMessage("LAST "+lastBlock.getJSONObject());
                    BlockPOW block = generator.getBlock(); //instead of synchronized{}
                    if(block.verifyWork()) {
                        Config.BlockFactory.parseBlock(block.getJSONObject(), (BlockPOW)lastBlock); //TODO remove assertion
                        //Logger.logDebugMessage("NEW BLOCK "+block.getJSONObject());
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
