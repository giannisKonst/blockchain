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


public final class GeneratorAgreement extends GeneratorPOW {

    private volatile String input;
    private volatile String actor;
    private volatile BlockPOW block;

    synchronized public void setInput(String input) {
        this.input = input;
        this.renewBlock();
    }

    synchronized public void setActor(String actor) {
        this.actor = actor;
        this.renewBlock();
    }

    synchronized public String getInput() {
        return input;
    }

    @Override
    synchronized BlockPOW getBlock() {
        return block;
    }

    @Override
    synchronized void renewBlock() {
        try{
            if(input != null && actor != null && getLastBlock() != null) {
                block = new BlockBA(input, actor, getValidTimestamp(), getLastBlock(), getTransactions());
            }else{
                Logger.logDebugMessage(input+actor+getLastBlock());
            }
        }catch(NxtException.ValidationException e){
            e.printStackTrace();
            throw new Error();
        }
    }


}
