package nxt;

import nxt.crypto.Crypto;
import nxt.db.DbIterator;
import nxt.db.DerivedDbTable;
import nxt.db.FilteringIterator;
import nxt.peer.Peer;
import nxt.peer.Peers;
import nxt.util.Convert;
import nxt.util.Filter;
import nxt.util.JSON;
import nxt.util.Listener;
import nxt.util.Listeners;
import nxt.util.Logger;
import nxt.util.ThreadPool;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONStreamAware;
import org.json.simple.JSONValue;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;

final class BlockchainProcessorImpl implements BlockchainProcessor {

    /*
    private static final byte[] CHECKSUM_TRANSPARENT_FORGING = new byte[]{27, -54, -59, -98, 49, -42, 48, -68, -112, 49, 41, 94, -41, 78, -84, 27, -87, -22, -28, 36, -34, -90, 112, -50, -9, 5, 89, -35, 80, -121, -128, 112};
    private static final byte[] CHECKSUM_NQT_BLOCK = Constants.isTestnet ?
            new byte[]{-126, -117, -94, -16, 125, -94, 38, 10, 11, 37, -33, 4, -70, -8, -40, -80, 18, -21, -54, -126, 109, -73, 63, -56, 67, 59, -30, 83, -6, -91, -24, 34}
            : new byte[]{-125, 17, 63, -20, 90, -98, 52, 114, 7, -100, -20, -103, -50, 76, 46, -38, -29, -43, -43, 45, 81, 12, -30, 100, -67, -50, -112, -15, 22, -57, 84, -106};
    private static final byte[] CHECKSUM_MONETARY_SYSTEM_BLOCK = Constants.isTestnet ?
            new byte[]{107, 104, 79, -12, -101, 15, 114, -78, -44, 106, -62, 56, 102, 25, 49, -105, 21, 113, -50, 122, -5, 36, 126, 7, 63, 71, 19, -7, 93, -84, 67, -79}
            : new byte[]{-54, -90, 113, -80, 17, -37, 44, -37, 80, 79, 107, -88, -60, -32, 93, 73, -60, 101, 102, -7, -5, -122, -93, -107, 63, 58, -125, -41, 26, -109, 51, -112};
    */

    private static final BlockchainProcessorImpl instance = new BlockchainProcessorImpl();

    static BlockchainProcessorImpl getInstance() {
        return instance;
    }

    private final BlockchainImpl blockchain = BlockchainImpl.getInstance();

    private final List<DerivedDbTable> derivedTables = new CopyOnWriteArrayList<>();
    private final boolean trimDerivedTables = Nxt.getBooleanProperty("nxt.trimDerivedTables");

    private volatile int lastTrimHeight;

    private final Listeners<Block, Event> blockListeners = new Listeners<>();
    private volatile Peer lastBlockchainFeeder = null;
    private volatile int lastBlockchainFeederHeight = 0;


    //private volatile boolean isScanning;
    private final Scanner scanner = new Scanner(blockListeners, derivedTables);
    private volatile boolean alreadyInitialized = false;

    private final GetBlocksFromPeers getMoreBlocksThread = new GetBlocksFromPeers();
    private final Generator generator = Generator.getInstance();

    private BlockchainProcessorImpl() {

        blockListeners.addListener(new Listener<Block>() {
            @Override
            public void notify(Block block) {
                if (block.getHeight() % 5000 == 0) {
                    Logger.logMessage("processed block " + block.getHeight());
                }
            }
        }, Event.BLOCK_SCANNED);

        blockListeners.addListener(new Listener<Block>() {
            @Override
            public void notify(Block block) {
                if (block.getHeight() % 5000 == 0) {
                    Logger.logMessage("received block " + block.getHeight());
                    Db.db.analyzeTables();
                }
                generator.setLastBlock(block);
            }
        }, Event.BLOCK_PUSHED);

        if (trimDerivedTables) {
            blockListeners.addListener(new Listener<Block>() {
                @Override
                public void notify(Block block) {
                    if (block.getHeight() % 1440 == 0) {
                        lastTrimHeight = Math.max(block.getHeight() - Constants.MAX_ROLLBACK, 0);
                        if (lastTrimHeight > 0) {
                            for (DerivedDbTable table : derivedTables) {
                                table.trim(lastTrimHeight);
                            }
                        }
                    }
                }
            }, Event.AFTER_BLOCK_APPLY);
        }

        blockListeners.addListener(new Listener<Block>() {
            @Override
            public void notify(Block block) {
                Db.db.analyzeTables();
            }
        }, Event.RESCAN_END);

        ThreadPool.runBeforeStart(new Runnable() {
            @Override
            public void run() {
                alreadyInitialized = true;
                addGenesisBlock();
                if (Nxt.getBooleanProperty("nxt.forceScan")) {
                    scan(0, Nxt.getBooleanProperty("nxt.forceValidate"));
                } else {
                    boolean rescan;
                    boolean validate;
                    int height;
                    try (Connection con = Db.db.getConnection();
                         Statement stmt = con.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT * FROM scan")) {
                        rs.next();
                        rescan = rs.getBoolean("rescan");
                        validate = rs.getBoolean("validate");
                        height = rs.getInt("height");
                    } catch (SQLException e) {
                        throw new RuntimeException(e.toString(), e);
                    }
                    if (rescan) {
                        scan(height, validate);
                    }
                }
            }
        }, false);

        ThreadPool.scheduleThread("GetMoreBlocks", getMoreBlocksThread, 1);
        getMoreBlocksThread.onBetterChain(new Listener<List<? extends BlockImpl>>() {
            @Override
            public void notify(List<? extends BlockImpl> chain) {
                handleBetterChain(chain);
            }
        });

		
	generator.onNewBlock(new Listener<Block>() {
	    @Override
	    public void notify(Block block) {
	        try{
                    synchronized(blockchain){
                        Block head = blockchain.getLastBlock();
                        if (block.getPreviousBlockId() == head.getId()) {
                            pushBlock((BlockImpl)block);
                        }else{
                            if (block.betterThan(head)) {
                                throw new Error("develop"); //TODO
                            }else{
                                Logger.logDebugMessage("rejected my block");
                            }
                        }
                    }
	        }catch(BlockNotAcceptedException e){
	            e.printStackTrace();
	            throw new Error(); //TODO
	        }
	    }
	});

    }

    @Override
    public boolean addListener(Listener<Block> listener, BlockchainProcessor.Event eventType) {
        return blockListeners.addListener(listener, eventType);
    }

    @Override
    public boolean removeListener(Listener<Block> listener, Event eventType) {
        return blockListeners.removeListener(listener, eventType);
    }

    @Override
    public void registerDerivedTable(DerivedDbTable table) {
        if (alreadyInitialized) {
            throw new IllegalStateException("Too late to register table " + table + ", must have done it in Nxt.Init");
        }
        derivedTables.add(table);
    }

    @Override
    public Peer getLastBlockchainFeeder() {
        return lastBlockchainFeeder;
    }

    @Override
    public int getLastBlockchainFeederHeight() {
        return lastBlockchainFeederHeight;
    }

    @Override
    public boolean isScanning() {
        return scanner.isScanning();
    }

    @Override
    public int getMinRollbackHeight() {
        return trimDerivedTables ? (lastTrimHeight > 0 ? lastTrimHeight : Math.max(blockchain.getHeight() - Constants.MAX_ROLLBACK, 0)) : 0;
    }

    @Override
    public void processPeerBlock(JSONObject request) throws NxtException {
        BlockImpl block = BlockImpl.parseBlock(request);
        pushBlock(block);
    }

    @Override
    public List<BlockImpl> popOffTo(int height) {
        if (height <= 0) {
            fullReset();
        } else if (height < getMinRollbackHeight()) {
            popOffWithRescan(height);
            return Collections.emptyList();
        } else if (height < blockchain.getHeight()) {
            return popOffTo(blockchain.getBlockAtHeight(height));
        }
        return Collections.emptyList();
    }

    @Override
    public void fullReset() {
        synchronized (blockchain) {
            try {
                setGetMoreBlocks(false);
                scheduleScan(0, false);
                //BlockDb.deleteBlock(Genesis.GENESIS_BLOCK_ID); // fails with stack overflow in H2
                BlockDb.deleteAll();
                addGenesisBlock();
                scan(0, false);
            } finally {
                setGetMoreBlocks(true);
            }
        }
    }

    @Override
    public void setGetMoreBlocks(boolean getMoreBlocks) {
        getMoreBlocksThread.setGetMoreBlocks(getMoreBlocks);
    }

    private void addBlock(BlockImpl block) {
        try (Connection con = Db.db.getConnection()) {
            BlockDb.saveBlock(con, block);
            blockchain.setLastBlock(block);
        } catch (SQLException e) {
            throw new RuntimeException(e.toString(), e);
        }
    }

    private void addGenesisBlock() {
        if (BlockDb.hasBlockAtHeight(0)) {
            Logger.logMessage("Genesis block already in database");
            BlockImpl lastBlock = BlockDb.findLastBlock();
            Logger.logMessage("Last block: " + lastBlock.getJSONObject());
            blockchain.setLastBlock(lastBlock);
            Logger.logMessage("Last block height: " + lastBlock.getHeight());
        }else{
            Logger.logMessage("Genesis block not in database, starting from scratch");
        //try {
            BlockImpl genesisBlock = (BlockImpl)BlockPOW.getGenesisBlock(); //TODO
            genesisBlock.setPrevious(null);
            addBlock(genesisBlock);
	/*
        } catch (NxtException.ValidationException e) {
            Logger.logMessage(e.getMessage());
            throw new RuntimeException(e.toString(), e);
        }*/
        }
        generator.startForging(blockchain.getLastBlock());
    }

    private void pushBlock(final BlockImpl block) throws BlockNotAcceptedException {

        int curTime = Nxt.getEpochTime();

        synchronized (blockchain) {
            TransactionProcessorImpl transactionProcessor = TransactionProcessorImpl.getInstance();
            BlockImpl previousLastBlock = null;
            try {
                Db.db.beginTransaction();
                previousLastBlock = blockchain.getLastBlock();

                Logger.logDebugMessage("pushBlock() "+block.getJSONObject());


                /*
                if (previousLastBlock.getId() != block.getPreviousBlockId()) {
                    throw new BlockOutOfOrderException("Previous block id doesn't match");
                }
		
                if (block.getVersion() != getBlockVersion(previousLastBlock.getHeight())) {
                    throw new BlockNotAcceptedException("Invalid version " + block.getVersion());
                }

                if (block.getVersion() != 1 && !Arrays.equals(Crypto.sha256().digest(previousLastBlock.getBytes()), block.getPreviousBlockHash())) {
                    throw new BlockNotAcceptedException("Previous block hash doesn't match");
                }
                if (block.getTimestamp() > curTime + 15 || block.getTimestamp() <= previousLastBlock.getTimestamp()) {
                    throw new BlockOutOfOrderException("Invalid timestamp: " + block.getTimestamp()
                            + " current time is " + curTime + ", previous block timestamp is " + previousLastBlock.getTimestamp());
                }
                */

                if (block.getId() == 0L || BlockDb.hasBlock(block.getId())) {
                    throw new BlockNotAcceptedException("Duplicate block or invalid id");
                }

                if (!block.verify() ) {
                    throw new BlockNotAcceptedException("Block verification failed");
                }
                if (!block.verify(previousLastBlock)) {
                    throw new BlockNotAcceptedException("Block verification failed");
                }


                Map<TransactionType, Map<String, Boolean>> duplicates = new HashMap<>();
                long calculatedTotalAmount = 0;
                long calculatedTotalFee = 0;
                MessageDigest digest = Crypto.sha256();

                for (TransactionImpl transaction : block.getTransactions()) {

                    transaction.validate2(); //TODO merge into one validate?

                    try {
                        transaction.validate();
                    } catch (NxtException.ValidationException e) {
                        throw new TransactionNotAcceptedException(e, transaction);
                    }

                    //TODO redundant
                    calculatedTotalAmount += transaction.getAmountNQT();
                    calculatedTotalFee += transaction.getFeeNQT();
                    digest.update(transaction.getBytes());
                }

                //TODO redundant
                if (calculatedTotalAmount != block.getTotalAmountNQT() || calculatedTotalFee != block.getTotalFeeNQT()) {
                    throw new BlockNotAcceptedException("Total amount or fee don't match transaction totals");
                }
                if (!Arrays.equals(digest.digest(), block.getPayloadHash())) {
                    throw new BlockNotAcceptedException("Payload hash doesn't match");
                }

                block.setPrevious(previousLastBlock); //TODO no need. perhaps replace with something usefull, a pre-persist phase
                blockListeners.notify(block, Event.BEFORE_BLOCK_ACCEPT);
                transactionProcessor.requeueAllUnconfirmedTransactions();
                addBlock(block);
                Logger.logDebugMessage("addBlock");
                apply(block);
                Logger.logDebugMessage("applyBlock");

                Db.db.commitTransaction();
            } catch (BlockNotAcceptedException e) {
                Db.db.rollbackTransaction();
                blockchain.setLastBlock(previousLastBlock);
                throw e;
            } finally {
                Db.db.endTransaction();
            }
        } // synchronized

        Logger.logDebugMessage("Event--BLOCK_PUSHED");
        blockListeners.notify(block, Event.BLOCK_PUSHED);

        if (block.getTimestamp() >= Nxt.getEpochTime() - 15) {
            Peers.sendToSomePeers(block);
        }

    }

    private void apply(BlockImpl block) throws TransactionNotAcceptedException {
        /* MOVED into block.apply()
        for (TransactionImpl transaction : block.getTransactions()) {
            if (! transaction.applyUnconfirmed()) {
                throw new TransactionNotAcceptedException("Double spending transaction: " + transaction.getStringId(), transaction);
            }
        }*/
        blockListeners.notify(block, Event.BEFORE_BLOCK_APPLY);
        block.apply();
        blockListeners.notify(block, Event.AFTER_BLOCK_APPLY);
        if (block.getTransactions().size() > 0) {
            TransactionProcessorImpl.getInstance().notifyListeners(block.getTransactions(), TransactionProcessor.Event.ADDED_CONFIRMED_TRANSACTIONS);
        }
    }

    private List<BlockImpl> popOffTo(Block commonBlock) {
        synchronized (blockchain) {
            /*
            if (commonBlock.getHeight() < getMinRollbackHeight()) {
                throw new IllegalArgumentException("Rollback to height " + commonBlock.getHeight() + " not supported, "
                        + "current height " + Nxt.getBlockchain().getHeight());
            }*/
            if (! blockchain.hasBlock(commonBlock.getId())) {
                Logger.logDebugMessage("Block " + commonBlock.getStringId() + " not found in blockchain, nothing to pop off");
                return Collections.emptyList();
            }
            List<BlockImpl> poppedOffBlocks = new ArrayList<>();
            try {
                Db.db.beginTransaction();
                BlockImpl block = blockchain.getLastBlock();
                block.getTransactions();
                Logger.logDebugMessage("Rollback from " + block.getHeight() + " to " + commonBlock.getHeight());
                while (block.getId() != commonBlock.getId() && block.getHeight() > 0 ) {
                    poppedOffBlocks.add(block);
                    block = popLastBlock();
                }
                for (DerivedDbTable table : derivedTables) {
                    table.rollback(commonBlock.getHeight());
                }
                Db.db.commitTransaction();
            } catch (RuntimeException e) {
                Db.db.rollbackTransaction();
                Logger.logDebugMessage("Error popping off to " + commonBlock.getHeight(), e);
                throw e;
            } finally {
                Db.db.endTransaction();
            }
            return poppedOffBlocks;
        } // synchronized
    }

    private BlockImpl popLastBlock() {
        BlockImpl block = blockchain.getLastBlock();
        if (block.getHeight() == 0) {
            throw new RuntimeException("Cannot pop off genesis block");
        }
        BlockImpl previousBlock = blockchain.getBlock(block.getPreviousBlockId());
        previousBlock.getTransactions();
        blockchain.setLastBlock(block, previousBlock);
        BlockDb.deleteBlocksFrom(block.getId());
        blockListeners.notify(block, Event.BLOCK_POPPED);
        return previousBlock;
    }

    private void popOffWithRescan(int height) {
        synchronized (blockchain) {
            try {
                BlockImpl block = BlockDb.findBlockAtHeight(height);
                scheduleScan(0, false);
                BlockDb.deleteBlocksFrom(block.getId());
                Logger.logDebugMessage("Deleted blocks starting from height %s", height);
            } finally {
                scan(0, false);
            }
        }
    }

    void scheduleScan(int height, boolean validate) {
        scanner.scheduleScan(height, validate);
    }

    @Override
    public void scan(int height, boolean validate) {
        scanner.scan(height, validate);
    }

    private void handleBetterChain(List<? extends BlockImpl> chain) {
        synchronized (blockchain) { //TODO

            long commonBlockId = chain.get(0).getPreviousBlockId();
            Block commonBlock = blockchain.getBlock(commonBlockId);
            if (!blockchain.hasBlock(commonBlockId)) { //my chain has changed in the meantime
                return; //TODO
            }
            //TODO is still better chain?

            List<BlockImpl> myChain;
            if (blockchain.getLastBlock().getId() != commonBlockId) {
                myChain = popOffTo(commonBlock);
            }else{
                myChain = new ArrayList<>(0);
            }
            Logger.logDebugMessage("poppedOff "+myChain.size()+" blocks");

            try{
                for (BlockImpl block : chain) {
                    pushBlock(block);
                }
            }catch(BlockNotAcceptedException|RuntimeException e){
                Logger.logDebugMessage("peer block rejeted", e);
                try {
                    for (BlockImpl block : myChain) {
                        pushBlock(block);
                    }
                } catch(BlockNotAcceptedException e2) {  //should never happen
                    throw new Error("develop", e2);
                }
                //TODO
                
            }
        }
    }

}
