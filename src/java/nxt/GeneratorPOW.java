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

    private Listener<Block> newBlockListener;
    private Block lastBlock;
    private List<Transaction> transactions;

    //private BigInteger nonce = 0; //should need persistance
    private BlockPOW block;

    public GeneratorPOW() {
    }

    public void init() {}

    public void startForging(Block lastBlock) {
        this.lastBlock = lastBlock;
        ThreadPool.scheduleThread("GenerateBlocks", generateBlocksThread, 500, TimeUnit.MILLISECONDS);
    }

    public void pauseForging() {
    }

    public void onNewBlock(Listener<Block> listener) {
        this.newBlockListener = listener;
    }

    public void setLastBlock(Block lastBlock) {
        this.lastBlock = lastBlock;
        block.setPrevious(lastBlock);
    }

    public void addTransaction(Transaction tx) {
        transactions.add(tx);
    }
    public void setTransactions(List<Transaction> txs) {
        transactions = txs;
    }

    private final Runnable generateBlocksThread = new Runnable() {

        private volatile int lastTimestamp;
        private volatile long lastBlockId;

        @Override
        public void run() {

            try {
                try {
                    int timestamp = Nxt.getEpochTime();
                    block.getHash();
                    if(block.verifyWork()) {
                        newBlockListener.notify(block);
                        setLastBlock(block);
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
