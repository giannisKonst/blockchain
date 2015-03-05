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


public abstract class Generator {

    /*
    private static final byte[] fakeForgingPublicKey;
    static {
        byte[] publicKey = null;
        if (Nxt.getBooleanProperty("nxt.enableFakeForging")) {
            Account fakeForgingAccount = Account.getAccount(Convert.parseAccountId(Nxt.getStringProperty("nxt.fakeForgingAccount")));
            if (fakeForgingAccount != null) {
                publicKey = fakeForgingAccount.getPublicKey();
            }
        }
        fakeForgingPublicKey = publicKey;
    }*/


    private static Generator generator;

    public static synchronized Generator getInstance() {
        if(generator == null) {
            try {
                generator = Config.generatorClass.newInstance();
            }catch(Throwable e) {
                throw new Error(e);
            }
        }
        return generator;
    }

    public abstract void init(); //? remove, use the constructor instead
    public abstract void setLastBlock(Block lastBlock); //block upon which to extend th chain
    public abstract void addTransaction(TransactionImpl tx);
    public abstract void setTransactions(List<TransactionImpl> txs);
    public abstract void startForging();
    public abstract void stopForging();
    public abstract void onNewBlock(Listener<Block> listener); //replace old listener?

    /*
    static boolean allowsFakeForging(byte[] publicKey) {
        return Constants.isTestnet && publicKey != null && Arrays.equals(publicKey, fakeForgingPublicKey);
    }*/


}
