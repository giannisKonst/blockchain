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

    private static final byte[] CHECKSUM_TRANSPARENT_FORGING = new byte[]{27, -54, -59, -98, 49, -42, 48, -68, -112, 49, 41, 94, -41, 78, -84, 27, -87, -22, -28, 36, -34, -90, 112, -50, -9, 5, 89, -35, 80, -121, -128, 112};
    private static final byte[] CHECKSUM_NQT_BLOCK = Constants.isTestnet ?
            new byte[]{-126, -117, -94, -16, 125, -94, 38, 10, 11, 37, -33, 4, -70, -8, -40, -80, 18, -21, -54, -126, 109, -73, 63, -56, 67, 59, -30, 83, -6, -91, -24, 34}
            : new byte[]{-125, 17, 63, -20, 90, -98, 52, 114, 7, -100, -20, -103, -50, 76, 46, -38, -29, -43, -43, 45, 81, 12, -30, 100, -67, -50, -112, -15, 22, -57, 84, -106};
    private static final byte[] CHECKSUM_MONETARY_SYSTEM_BLOCK = Constants.isTestnet ?
            new byte[]{107, 104, 79, -12, -101, 15, 114, -78, -44, 106, -62, 56, 102, 25, 49, -105, 21, 113, -50, 122, -5, 36, 126, 7, 63, 71, 19, -7, 93, -84, 67, -79}
            : new byte[]{-54, -90, 113, -80, 17, -37, 44, -37, 80, 79, 107, -88, -60, -32, 93, 73, -60, 101, 102, -7, -5, -122, -93, -107, 63, 58, -125, -41, 26, -109, 51, -112};

    private static final BlockchainProcessorImpl instance = new BlockchainProcessorImpl();

    static BlockchainProcessorImpl getInstance() {
        return instance;
    }

    private final BlockchainImpl blockchain = BlockchainImpl.getInstance();

    private final List<DerivedDbTable> derivedTables = new CopyOnWriteArrayList<>();
    private final boolean trimDerivedTables = Nxt.getBooleanProperty("nxt.trimDerivedTables");

    private volatile int lastTrimHeight;

    private final Listeners<Block, Event> blockListeners = new Listeners<>();
    private volatile Peer lastBlockchainFeeder;
    private volatile int lastBlockchainFeederHeight;


    private volatile boolean isScanning;
    private volatile boolean alreadyInitialized = false;

    private final GetBlocksFromPeers getMoreBlocksThread = new GetBlocksFromPeers();

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
                if (block.getHeight() == Constants.TRANSPARENT_FORGING_BLOCK && ! verifyChecksum(CHECKSUM_TRANSPARENT_FORGING)) {
                    popOffTo(0);
                }
                if (block.getHeight() == Constants.NQT_BLOCK && ! verifyChecksum(CHECKSUM_NQT_BLOCK)) {
                    popOffTo(Constants.TRANSPARENT_FORGING_BLOCK);
                }
                if (block.getHeight() == Constants.MONETARY_SYSTEM_BLOCK && ! verifyChecksum(CHECKSUM_MONETARY_SYSTEM_BLOCK)) {
                    popOffTo(Constants.NQT_BLOCK);
                }
            }
        }, Event.BLOCK_PUSHED);

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
        return isScanning;
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
        if (BlockDb.hasBlock(Genesis.GENESIS_BLOCK_ID)) {
            Logger.logMessage("Genesis block already in database");
            BlockImpl lastBlock = BlockDb.findLastBlock();
            blockchain.setLastBlock(lastBlock);
            Logger.logMessage("Last block height: " + lastBlock.getHeight());
            return;
        }
        Logger.logMessage("Genesis block not in database, starting from scratch");
        try {
            BlockImpl genesisBlock = (BlockImpl)BlockNXTImpl.getGenesisBlock(); //TODO
            genesisBlock.setPrevious(null);
            addBlock(genesisBlock);
        } catch (NxtException.ValidationException e) {
            Logger.logMessage(e.getMessage());
            throw new RuntimeException(e.toString(), e);
        }
    }

    public void pushBlock(final BlockImpl block) throws BlockNotAcceptedException {

        int curTime = Nxt.getEpochTime();

        synchronized (blockchain) {
            TransactionProcessorImpl transactionProcessor = TransactionProcessorImpl.getInstance();
            BlockImpl previousLastBlock = null;
            try {
                Db.db.beginTransaction();
                previousLastBlock = blockchain.getLastBlock();

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
                if (block.getId() == 0L || BlockDb.hasBlock(block.getId())) {
                    throw new BlockNotAcceptedException("Duplicate block or invalid id");
                }

                if (!block.verify()) {
                    throw new BlockNotAcceptedException("Block verification failed");
                }

                Map<TransactionType, Map<String, Boolean>> duplicates = new HashMap<>();
                long calculatedTotalAmount = 0;
                long calculatedTotalFee = 0;
                MessageDigest digest = Crypto.sha256();

                for (TransactionImpl transaction : block.getTransactions()) {

                    if (transaction.getTimestamp() > curTime + 15) {
                        throw new BlockOutOfOrderException("Invalid transaction timestamp: " + transaction.getTimestamp()
                                + ", current time is " + curTime);
                    }
                    // cfb: Block 303 contains a transaction which expired before the block timestamp
                    if (transaction.getTimestamp() > block.getTimestamp() + 15
                            || (transaction.getExpiration() < block.getTimestamp() && previousLastBlock.getHeight() != 303)) {
                        throw new TransactionNotAcceptedException("Invalid transaction timestamp " + transaction.getTimestamp()
                                + " for transaction " + transaction.getStringId() + ", current time is " + curTime
                                + ", block timestamp is " + block.getTimestamp(), transaction);
                    }
                    if (TransactionDb.hasTransaction(transaction.getId())) {
                        throw new TransactionNotAcceptedException("Transaction " + transaction.getStringId()
                                + " is already in the blockchain", transaction);
                    }
                    if (transaction.getReferencedTransactionFullHash() != null) {
                        if ((previousLastBlock.getHeight() < Constants.REFERENCED_TRANSACTION_FULL_HASH_BLOCK
                                && !TransactionDb.hasTransaction(Convert.fullHashToId(transaction.getReferencedTransactionFullHash())))
                                || (previousLastBlock.getHeight() >= Constants.REFERENCED_TRANSACTION_FULL_HASH_BLOCK
                                && !TransactionProcessorImpl.hasAllReferencedTransactions(transaction, transaction.getTimestamp(), 0))) {
                            throw new TransactionNotAcceptedException("Missing or invalid referenced transaction "
                                    + transaction.getReferencedTransactionFullHash()
                                    + " for transaction " + transaction.getStringId(), transaction);
                        }
                    }
                    if (transaction.getVersion() != transactionProcessor.getTransactionVersion(previousLastBlock.getHeight())) {
                        throw new TransactionNotAcceptedException("Invalid transaction version " + transaction.getVersion()
                                + " at height " + previousLastBlock.getHeight(), transaction);
                    }
                    if (!transaction.verifySignature()) {
                        throw new TransactionNotAcceptedException("Signature verification failed for transaction "
                                + transaction.getStringId() + " at height " + previousLastBlock.getHeight(), transaction);
                    }
                    /*
                    if (!EconomicClustering.verifyFork(transaction)) {
                        Logger.logDebugMessage("Block " + block.getStringId() + " height " + (previousLastBlock.getHeight() + 1)
                                + " contains transaction that was generated on a fork: "
                                + transaction.getStringId() + " ecBlockHeight " + transaction.getECBlockHeight() + " ecBlockId "
                                + Convert.toUnsignedLong(transaction.getECBlockId()));
                        //throw new TransactionNotAcceptedException("Transaction belongs to a different fork", transaction);
                    }
                    */
                    if (transaction.getId() == 0L) {
                        throw new TransactionNotAcceptedException("Invalid transaction id", transaction);
                    }
                    try {
                        transaction.validate();
                    } catch (NxtException.ValidationException e) {
                        throw new TransactionNotAcceptedException(e, transaction);
                    }
                    if (transaction.isDuplicate(duplicates)) {
                        throw new TransactionNotAcceptedException("Transaction is a duplicate: "
                                + transaction.getStringId(), transaction);
                    }

                    calculatedTotalAmount += transaction.getAmountNQT();

                    calculatedTotalFee += transaction.getFeeNQT();

                    digest.update(transaction.getBytes());

                }

                if (calculatedTotalAmount != block.getTotalAmountNQT() || calculatedTotalFee != block.getTotalFeeNQT()) {
                    throw new BlockNotAcceptedException("Total amount or fee don't match transaction totals");
                }
                if (!Arrays.equals(digest.digest(), block.getPayloadHash())) {
                    throw new BlockNotAcceptedException("Payload hash doesn't match");
                }

                block.setPrevious(previousLastBlock);
                blockListeners.notify(block, Event.BEFORE_BLOCK_ACCEPT);
                transactionProcessor.requeueAllUnconfirmedTransactions();
                addBlock(block);
                accept(block);

                Db.db.commitTransaction();
            } catch (Exception e) {
                Db.db.rollbackTransaction();
                blockchain.setLastBlock(previousLastBlock);
                throw e;
            } finally {
                Db.db.endTransaction();
            }
        } // synchronized

        blockListeners.notify(block, Event.BLOCK_PUSHED);

        if (block.getTimestamp() >= Nxt.getEpochTime() - 15) {
            Peers.sendToSomePeers(block);
        }

    }

    private void accept(BlockImpl block) throws TransactionNotAcceptedException {
        for (TransactionImpl transaction : block.getTransactions()) {
            if (! transaction.applyUnconfirmed()) {
                throw new TransactionNotAcceptedException("Double spending transaction: " + transaction.getStringId(), transaction);
            }
        }
        blockListeners.notify(block, Event.BEFORE_BLOCK_APPLY);
        block.apply();
        blockListeners.notify(block, Event.AFTER_BLOCK_APPLY);
        if (block.getTransactions().size() > 0) {
            TransactionProcessorImpl.getInstance().notifyListeners(block.getTransactions(), TransactionProcessor.Event.ADDED_CONFIRMED_TRANSACTIONS);
        }
    }

    private List<BlockImpl> popOffTo(Block commonBlock) {
        synchronized (blockchain) {
            if (commonBlock.getHeight() < getMinRollbackHeight()) {
                throw new IllegalArgumentException("Rollback to height " + commonBlock.getHeight() + " not supported, "
                        + "current height " + Nxt.getBlockchain().getHeight());
            }
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
                while (block.getId() != commonBlock.getId() && block.getId() != Genesis.GENESIS_BLOCK_ID) {
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
        if (block.getId() == Genesis.GENESIS_BLOCK_ID) {
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

    int getBlockVersion(int previousBlockHeight) {
        return previousBlockHeight < Constants.TRANSPARENT_FORGING_BLOCK ? 1
                : previousBlockHeight < Constants.NQT_BLOCK ? 2
                : 3;
    }

    private boolean verifyChecksum(byte[] validChecksum) {
        MessageDigest digest = Crypto.sha256();
        try (Connection con = Db.db.getConnection();
             PreparedStatement pstmt = con.prepareStatement(
                     "SELECT * FROM transaction ORDER BY id ASC, timestamp ASC");
             DbIterator<TransactionImpl> iterator = blockchain.getTransactions(con, pstmt)) {
            while (iterator.hasNext()) {
                digest.update(iterator.next().getBytes());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e.toString(), e);
        }
        byte[] checksum = digest.digest();
        if (validChecksum == null) {
            Logger.logMessage("Checksum calculated:\n" + Arrays.toString(checksum));
            return true;
        } else if (!Arrays.equals(checksum, validChecksum)) {
            Logger.logErrorMessage("Checksum failed at block " + blockchain.getHeight() + ": " + Arrays.toString(checksum));
            return false;
        } else {
            Logger.logMessage("Checksum passed at block " + blockchain.getHeight());
            return true;
        }
    }

    void scheduleScan(int height, boolean validate) {
        try (Connection con = Db.db.getConnection();
             PreparedStatement pstmt = con.prepareStatement("UPDATE scan SET rescan = TRUE, height = ?, validate = ?")) {
            pstmt.setInt(1, height);
            pstmt.setBoolean(2, validate);
            pstmt.executeUpdate();
            Logger.logDebugMessage("Scheduled scan starting from height " + height + (validate ? ", with validation" : ""));
        } catch (SQLException e) {
            throw new RuntimeException(e.toString(), e);
        }
    }

    @Override
    public void scan(int height, boolean validate) {
        scheduleScan(height, validate);
        synchronized (blockchain) {
            TransactionProcessorImpl transactionProcessor = TransactionProcessorImpl.getInstance();
            int blockchainHeight = Nxt.getBlockchain().getHeight();
            if (height > blockchainHeight + 1) {
                throw new IllegalArgumentException("Rollback height " + (height - 1) + " exceeds current blockchain height of " + blockchainHeight);
            }
            if (height > 0 && height < getMinRollbackHeight()) {
                Logger.logMessage("Rollback of more than " + Constants.MAX_ROLLBACK + " blocks not supported, will do a full scan");
                height = 0;
            }
            if (height < 0) {
                height = 0;
            }
            Logger.logMessage("Scanning blockchain starting from height " + height + "...");
            if (validate) {
                Logger.logDebugMessage("Also verifying signatures and validating transactions...");
            }
            try (Connection con = Db.db.beginTransaction();
                 PreparedStatement pstmtSelect = con.prepareStatement("SELECT * FROM block WHERE height >= ? ORDER BY db_id ASC");
                 PreparedStatement pstmtDone = con.prepareStatement("UPDATE scan SET rescan = FALSE, height = 0, validate = FALSE")) {
                isScanning = true;
                transactionProcessor.requeueAllUnconfirmedTransactions();
                for (DerivedDbTable table : derivedTables) {
                    if (height == 0) {
                        table.truncate();
                    } else {
                        table.rollback(height - 1);
                    }
                }
                Db.db.commitTransaction();
                Logger.logDebugMessage("Rolled back derived tables");
                pstmtSelect.setInt(1, height);
                BlockImpl currentBlock = BlockDb.findBlockAtHeight(height);
                blockListeners.notify(currentBlock, Event.RESCAN_BEGIN);
                long currentBlockId = currentBlock.getId();
                if (height == 0) {
                    blockchain.setLastBlock(currentBlock); // special case to avoid no last block
                    Account.addOrGetAccount(Genesis.CREATOR_ID).apply(Genesis.CREATOR_PUBLIC_KEY, 0);
                } else {
                    blockchain.setLastBlock(BlockDb.findBlockAtHeight(height - 1));
                }
                try (ResultSet rs = pstmtSelect.executeQuery()) {
                    while (rs.next()) {
                        try {
                            currentBlock = BlockDb.loadBlock(con, rs);
                            if (currentBlock.getId() != currentBlockId) {
                                throw new NxtException.NotValidException("Database blocks in the wrong order!");
                            }
                            if (validate && currentBlockId != Genesis.GENESIS_BLOCK_ID) {
				/*
                                if (!currentBlock.verifyBlockSignature()) {
                                    throw new NxtException.NotValidException("Invalid block signature");
                                }
                                if (!currentBlock.verifyGenerationSignature() && !Generator.allowsFakeForging(currentBlock.getGeneratorPublicKey())) {
                                    throw new NxtException.NotValidException("Invalid block generation signature");
                                }
                                if (currentBlock.getVersion() != getBlockVersion(blockchain.getHeight())) {
                                    throw new NxtException.NotValidException("Invalid block version");
                                }*/
                                if (!currentBlock.verify()) {
                                    throw new NxtException.NotValidException("Invalid block");
                                }


                                byte[] blockBytes = currentBlock.getBytes();
                                JSONObject blockJSON = (JSONObject) JSONValue.parse(currentBlock.getJSONObject().toJSONString());
                                if (!Arrays.equals(blockBytes, BlockImpl.parseBlock(blockJSON).getBytes())) {
                                    throw new NxtException.NotValidException("Block JSON cannot be parsed back to the same block");
                                }
                                Map<TransactionType, Map<String, Boolean>> duplicates = new HashMap<>();
                                for (TransactionImpl transaction : currentBlock.getTransactions()) {
                                    if (!transaction.verifySignature()) {
                                        throw new NxtException.NotValidException("Invalid transaction signature");
                                    }
                                    if (transaction.getVersion() != transactionProcessor.getTransactionVersion(blockchain.getHeight())) {
                                        throw new NxtException.NotValidException("Invalid transaction version");
                                    }
                                    /*
                                    if (!EconomicClustering.verifyFork(transaction)) {
                                        Logger.logDebugMessage("Found transaction that was generated on a fork: " + transaction.getStringId()
                                                + " in block " + currentBlock.getStringId() + " at height " + currentBlock.getHeight()
                                                + " ecBlockHeight " + transaction.getECBlockHeight() + " ecBlockId " + Convert.toUnsignedLong(transaction.getECBlockId()));
                                        //throw new NxtException.NotValidException("Invalid transaction fork");
                                    }
                                    */
                                    transaction.validate();
                                    if (transaction.isDuplicate(duplicates)) {
                                        throw new NxtException.NotValidException("Transaction is a duplicate: " + transaction.getStringId());
                                    }
                                    byte[] transactionBytes = transaction.getBytes();
                                    if (currentBlock.getHeight() > Constants.NQT_BLOCK
                                            && !Arrays.equals(transactionBytes, transactionProcessor.parseTransaction(transactionBytes).getBytes())) {
                                        throw new NxtException.NotValidException("Transaction bytes cannot be parsed back to the same transaction");
                                    }
                                    JSONObject transactionJSON = (JSONObject) JSONValue.parse(transaction.getJSONObject().toJSONString());
                                    if (!Arrays.equals(transactionBytes, transactionProcessor.parseTransaction(transactionJSON).getBytes())) {
                                        throw new NxtException.NotValidException("Transaction JSON cannot be parsed back to the same transaction");
                                    }
                                }
                            }
                            blockListeners.notify(currentBlock, Event.BEFORE_BLOCK_ACCEPT);
                            blockchain.setLastBlock(currentBlock);
                            accept(currentBlock);
                            currentBlockId = currentBlock.getNextBlockId();
                            Db.db.commitTransaction();
                        } catch (NxtException | RuntimeException e) {
                            Db.db.rollbackTransaction();
                            Logger.logDebugMessage(e.toString(), e);
                            Logger.logDebugMessage("Applying block " + Convert.toUnsignedLong(currentBlockId) + " at height "
                                    + (currentBlock == null ? 0 : currentBlock.getHeight()) + " failed, deleting from database");
                            if (currentBlock != null) {
                                transactionProcessor.processLater(currentBlock.getTransactions());
                            }
                            while (rs.next()) {
                                try {
                                    currentBlock = BlockDb.loadBlock(con, rs);
                                    transactionProcessor.processLater(currentBlock.getTransactions());
                                } catch (NxtException.ValidationException ignore) {
                                }
                            }
                            BlockDb.deleteBlocksFrom(currentBlockId);
                            blockchain.setLastBlock(BlockDb.findLastBlock());
                        }
                        blockListeners.notify(currentBlock, Event.BLOCK_SCANNED);
                    }
                }
                pstmtDone.executeUpdate();
                Db.db.commitTransaction();
                blockListeners.notify(currentBlock, Event.RESCAN_END);
                Logger.logMessage("...done at height " + Nxt.getBlockchain().getHeight());
            } catch (SQLException e) {
                throw new RuntimeException(e.toString(), e);
            } finally {
                Db.db.endTransaction();
                isScanning = false;
            }
        } // synchronized
    }

}
