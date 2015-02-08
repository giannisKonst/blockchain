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


public final class GeneratorNXT extends Generator implements Comparable<GeneratorNXT> {

    public static enum Event {
        GENERATION_DEADLINE, START_FORGING, STOP_FORGING
    }

    private static final Runnable generateBlocksThread = new Runnable() {

        private volatile int lastTimestamp;
        private volatile long lastBlockId;

        @Override
        public void run() {

            try {
                try {
                    int timestamp = Nxt.getEpochTime();
                    if (timestamp == lastTimestamp) {
                        return;
                    }
                    lastTimestamp = timestamp;
                    synchronized (Nxt.getBlockchain()) { //TODO
                        Block lastBlock = Nxt.getBlockchain().getLastBlock(); //TODO
                        if (lastBlock == null || lastBlock.getHeight() < Constants.LAST_KNOWN_BLOCK) {
                            return;
                        }
                        if (lastBlock.getId() != lastBlockId || sortedForgers == null) {
                            lastBlockId = lastBlock.getId();
                            List<GeneratorNXT> forgers = new ArrayList<>();
                            for (GeneratorNXT generator : generators.values()) {
                                generator.setLastBlock(lastBlock);
                                if (generator.effectiveBalance.signum() > 0) {
                                    forgers.add(generator);
                                }
                            }
                            Collections.sort(forgers);
                            sortedForgers = Collections.unmodifiableList(forgers);
                        }
                        for (GeneratorNXT generator : sortedForgers) {
                            if (generator.getHitTime() > timestamp + 1 || generator.forge(lastBlock, timestamp)) {
                                return;
                            }
                        }
                    } // synchronized
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


    private static final ConcurrentMap<String, GeneratorNXT> generators = new ConcurrentHashMap<>();
    private static final Collection<GeneratorNXT> allGenerators = Collections.unmodifiableCollection(generators.values());
    private static volatile List<GeneratorNXT> sortedForgers;

    private static Listener<Block> newBlockListener;
    private static final Listeners<GeneratorNXT,Event> listeners = new Listeners<>();

    public GeneratorNXT() {
    }
    public void init() {}
    public void startForging(Block lastBlock) {
        //TODO lastBlock
        ThreadPool.scheduleThread("GenerateBlocks", generateBlocksThread, 500, TimeUnit.MILLISECONDS);
    }
    public void pauseForging() {
        //TODO
    }
    public void onNewBlock(Listener<Block> listener) {
        this.newBlockListener = listener;
    }

    public void addTransaction(Transaction tx) {} //TODO
    public void setTransactions(List<Transaction> txs) {} //TODO

    public static boolean addListener(Listener<GeneratorNXT> listener, Event eventType) {
        return listeners.addListener(listener, eventType);
    }

    public static boolean removeListener(Listener<GeneratorNXT> listener, Event eventType) {
        return listeners.removeListener(listener, eventType);
    }


    public static GeneratorNXT startForging(String secretPhrase) {
        GeneratorNXT generator = new GeneratorNXT(secretPhrase);
        GeneratorNXT old = generators.putIfAbsent(secretPhrase, generator);
        if (old != null) {
            Logger.logDebugMessage("Account " + Convert.toUnsignedLong(old.getAccountId()) + " is already forging");
            return old;
        }
        listeners.notify(generator, Event.START_FORGING);
        Logger.logDebugMessage("Account " + Convert.toUnsignedLong(generator.getAccountId()) + " started forging, deadline "
                + generator.getDeadline() + " seconds");
        return generator;
    }

    public static GeneratorNXT stopForging(String secretPhrase) {
        GeneratorNXT generator = generators.remove(secretPhrase);
        if (generator != null) {
            sortedForgers = null;
            Logger.logDebugMessage("Account " + Convert.toUnsignedLong(generator.getAccountId()) + " stopped forging");
            listeners.notify(generator, Event.STOP_FORGING);
        }
        return generator;
    }

    public static GeneratorNXT getGenerator(String secretPhrase) {
        return generators.get(secretPhrase);
    }

    public static Collection<GeneratorNXT> getAllGenerators() {
        return allGenerators;
    }






    private final long accountId;
    private final String secretPhrase;
    private final byte[] publicKey;
    private volatile long hitTime;
    private volatile BigInteger hit;
    private volatile BigInteger effectiveBalance;

    private GeneratorNXT(String secretPhrase) {
        this.secretPhrase = secretPhrase;
        this.publicKey = Crypto.getPublicKey(secretPhrase);
        this.accountId = Account.getId(publicKey);
        if (Nxt.getBlockchain().getHeight() >= Constants.LAST_KNOWN_BLOCK) {
            setLastBlock(Nxt.getBlockchain().getLastBlock());
        }
        sortedForgers = null;
    }

    public byte[] getPublicKey() {
        return publicKey;
    }

    public long getAccountId() {
        return accountId;
    }

    public long getDeadline() {
        return Math.max(hitTime - Nxt.getBlockchain().getLastBlock().getTimestamp(), 0);
    }

    public long getHitTime() {
        return hitTime;
    }

    @Override
    public int compareTo(GeneratorNXT g) {
        int i = this.hit.multiply(g.effectiveBalance).compareTo(g.hit.multiply(this.effectiveBalance));
        if (i != 0) {
            return i;
        }
        return Long.compare(accountId, g.accountId);
    }

    @Override
    public String toString() {
        return "account: " + Convert.toUnsignedLong(accountId) + " deadline: " + getDeadline();
    }

    public void setLastBlock(Block lastBlock) {
        Account account = Account.getAccount(accountId);
        effectiveBalance = BigInteger.valueOf(account == null || account.getEffectiveBalanceNXT() <= 0 ? 0 : account.getEffectiveBalanceNXT());
        if (effectiveBalance.signum() == 0) {
            return;
        }
        hit = ProofOfNXT.getHit(publicKey, lastBlock);
        hitTime = ProofOfNXT.getHitTime(effectiveBalance, hit, lastBlock);
        listeners.notify(this, Event.GENERATION_DEADLINE);
    }

    private boolean forge(Block lastBlock, int timestamp) throws BlockchainProcessor.BlockNotAcceptedException {
        if (ProofOfNXT.verifyHit(hit, effectiveBalance, lastBlock, timestamp)) {
            while (true) {
                try {
                    //BlockchainProcessorImpl.getInstance().generateBlock(secretPhrase, timestamp);
                    this.generateBlock(lastBlock, secretPhrase, timestamp);
                    return true;
                } catch (BlockchainProcessor.TransactionNotAcceptedException e) {
                    if (Nxt.getEpochTime() - timestamp > 10) {
                        throw e;
                    }
                }
            }
        }
        return false;
    }

    private void generateBlock(Block previousBlock, String secretPhrase, int blockTimestamp) throws BlockNotAcceptedException {

        TransactionProcessorImpl transactionProcessor = TransactionProcessorImpl.getInstance();

	SortedSet<UnconfirmedTransaction> sortedTransactions = transactionProcessor.assembleBlockTransactions();

        List<Transaction> blockTransactions = new ArrayList<>();
        for (UnconfirmedTransaction unconfirmedTransaction : sortedTransactions) {
            blockTransactions.add(unconfirmedTransaction.getTransaction());
        }

        final byte[] publicKey = Crypto.getPublicKey(secretPhrase);

	BlockNXTImpl block;

        try {
            /* block = new BlockImpl(getBlockVersion(previousBlock.getHeight()), blockTimestamp, previousBlock.getId(), totalAmountNQT, totalFeeNQT, payloadLength,
                    payloadHash, publicKey, generationSignature, null, previousBlockHash, blockTransactions);
		*/

            block = new BlockNXTImpl(blockTimestamp, previousBlock, publicKey, blockTransactions);

        } catch (NxtException.ValidationException e) {
            // shouldn't happen because all transactions are already validated
            Logger.logMessage("Error generating block", e);
            return;
        }

        block.sign(secretPhrase);

        try {
            BlockchainProcessorImpl.getInstance().pushBlock(block);
            //blockListeners.notify(block, Event.BLOCK_GENERATED);
            Logger.logDebugMessage("Account " + Convert.toUnsignedLong(block.getGeneratorId()) + " generated block " + block.getStringId()
                    + " at height " + block.getHeight());
        } catch (TransactionNotAcceptedException e) {
            Logger.logDebugMessage("Generate block failed: " + e.getMessage());
            Transaction transaction = e.getTransaction();
            Logger.logDebugMessage("Removing invalid transaction: " + transaction.getStringId());
            transactionProcessor.removeUnconfirmedTransaction((TransactionImpl) transaction);
            throw e;
        } catch (BlockNotAcceptedException e) {
            Logger.logDebugMessage("Generate block failed: " + e.getMessage());
            throw e;
        }
    }



}
