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

import nxt.BlockchainProcessor.Event;
import nxt.BlockchainProcessor.TransactionNotAcceptedException;

class Scanner {

    private final BlockchainImpl blockchain = BlockchainImpl.getInstance();
    private volatile boolean isScanning;

    private final Listeners<Block, Event> blockListeners;
    private final List<DerivedDbTable> derivedTables;

    Scanner(Listeners<Block, Event> blockListeners, List<DerivedDbTable> derivedTables) {
        this.blockListeners = blockListeners;
        this.derivedTables = derivedTables;
    }

    boolean isScanning() {
        return isScanning;
    }

    private int getMinRollbackHeight() {
        return 0; //TODO
        //return trimDerivedTables ? (lastTrimHeight > 0 ? lastTrimHeight : Math.max(blockchain.getHeight() - Constants.MAX_ROLLBACK, 0)) : 0;
    }

    private void apply(BlockImpl block) throws TransactionNotAcceptedException {
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

    //@Override
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
                            if (validate && currentBlock.getHeight() > 0) {
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
                            apply(currentBlock);
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
