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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONStreamAware;
import org.json.simple.JSONValue;

import java.math.BigInteger;
import java.util.List;

import nxt.util.JSON;
import nxt.peer.Peer;
import nxt.BlockchainProcessor.BlockNotAcceptedException;

public class GetBlocksFromPeers implements Runnable {

        private final JSONStreamAware getCumulativeDifficultyRequest;

        {
            JSONObject request = new JSONObject();
            request.put("requestType", "getCumulativeDifficulty");
            getCumulativeDifficultyRequest = JSON.prepareRequest(request);
        }
        private BlockchainProcessorImpl chainProcessor = BlockchainProcessorImpl.getInstance();
        private final BlockchainImpl blockchain = BlockchainImpl.getInstance();
        private final int defaultNumberOfForkConfirmations = Nxt.getIntProperty(Constants.isTestnet ? "nxt.testnetNumberOfForkConfirmations" : "nxt.numberOfForkConfirmations");
        private boolean peerHasMore;

        private volatile boolean getMoreBlocks = true;
        private volatile Listener<List<? extends BlockImpl>> betterChainListener;

        public void setGetMoreBlocks(boolean getMoreBlocks) {
            this.getMoreBlocks = getMoreBlocks;
        }

        public void onBetterChain(Listener<List<? extends BlockImpl>> listener) {
            this.betterChainListener = listener;
        }

        @Override
        public void run() {
            //Logger.logDebugMessage("getBlocksFromPeers run() "+getMoreBlocks);
            try {
                try {
                    if (!getMoreBlocks) {
                        return;
                    }
                    //List<Peer> connectedPublicPeers = Peers.getPublicPeers(Peer.State.CONNECTED, true);
                    List<Peer> connectedPeers = Peers.getPeers(Peer.State.CONNECTED);
			/*
                    int numberOfForkConfirmations = blockchain.getHeight() > Constants.MONETARY_SYSTEM_BLOCK - 720 ?
                            defaultNumberOfForkConfirmations : Math.min(1, defaultNumberOfForkConfirmations);
                    if (connectedPublicPeers.size() <= numberOfForkConfirmations) {
                        return;
                    }*/

                    peerHasMore = true;
                    final Peer peer = Peers.getWeightedPeer(connectedPeers);
                    Logger.logDebugMessage("getBlocksFromPeers run() "+connectedPeers.size()+" "+(peer==null ?null :peer.getPeerAddress()));
                    if (peer == null) {
                        return;
                    }

                    if( advertisesBetterChain(peer) == false ) {
                        return;
                    }

                    long commonMilestoneBlockId = Nxt.getGenesisBlockId();

                    //if (blockchain.getLastBlock().getId() != Nxt.getGenesisBlockId()) {
                        commonMilestoneBlockId = getCommonMilestoneBlockId(peer);
                    //}
                    if (commonMilestoneBlockId == 0 || !peerHasMore) {
                        return;
                    }
                    Logger.logDebugMessage("getBlocksFromPeers commonMilestoneBlockId "+commonMilestoneBlockId);

                    final long commonBlockId = getCommonBlockId(peer, commonMilestoneBlockId);
                    if (commonBlockId == 0 || !peerHasMore) {
                        return;
                    }
                    Logger.logDebugMessage("getBlocksFromPeers b "+commonMilestoneBlockId);

                    final Block commonBlock = blockchain.getBlock(commonBlockId);
                    /*
                    if (commonBlock == null || blockchain.getHeight() - commonBlock.getHeight() >= 720) {
                        return;
                    }*/
                    Logger.logDebugMessage("getBlocksFromPeers b "+commonMilestoneBlockId);

                    synchronized (blockchain) {
                        long lastBlockId = blockchain.getLastBlock().getId();
                        downloadBlockchain(peer, commonBlock);
                    } // synchronized

                } catch (NxtException.StopException e) {
                    Logger.logMessage("Blockchain download stopped: " + e.getMessage());
                } catch (Exception e) {
                    Logger.logDebugMessage("Error in blockchain download thread", e);
                }
            } catch (Throwable t) {
                Logger.logMessage("CRITICAL ERROR. PLEASE REPORT TO THE DEVELOPERS.\n" + t.toString());
                t.printStackTrace();
                System.exit(1);
            }

        }

        private boolean advertisesBetterChain(Peer peer) {
            JSONObject response = peer.send(getCumulativeDifficultyRequest);
            Logger.logDebugMessage("advertisesBetterChain "+response);
            if (response == null) {
                return false;
            }

            BigInteger myCumulativeDifficulty = ((BlockPOW)blockchain.getLastBlock()).getCumulativeDifficulty();
            String peerCumulativeDifficulty = (String) response.get("cumulativeDifficulty");
            if (peerCumulativeDifficulty == null) {
                return false;
            }
            BigInteger betterCumulativeDifficulty = new BigInteger(peerCumulativeDifficulty);
            return betterCumulativeDifficulty.compareTo(myCumulativeDifficulty) == 1;
            /*
            if (response.get("blockchainHeight") != null) {
                lastBlockchainFeeder = peer;
                lastBlockchainFeederHeight = ((Long) response.get("blockchainHeight")).intValue();
            }
            if (betterCumulativeDifficulty.equals(myCumulativeDifficulty)) {
                return false;
            }
	    */

        }

        private long getCommonMilestoneBlockId(Peer peer) {

            String lastMilestoneBlockId = null;

            while (true) {
                JSONObject milestoneBlockIdsRequest = new JSONObject();
                milestoneBlockIdsRequest.put("requestType", "getMilestoneBlockIds");
                if (lastMilestoneBlockId == null) {
                    milestoneBlockIdsRequest.put("lastBlockId", blockchain.getLastBlock().getStringId());
                } else {
                    milestoneBlockIdsRequest.put("lastMilestoneBlockId", lastMilestoneBlockId);
                }

                JSONObject response = peer.send(JSON.prepareRequest(milestoneBlockIdsRequest));
                if (response == null) {
                    return 0;
                }
                Logger.logDebugMessage("getCommonMilestoneBlockId() resp= "+response);
                JSONArray milestoneBlockIds = (JSONArray) response.get("milestoneBlockIds");
                if (milestoneBlockIds == null) {
                    return 0;
                }
                if (milestoneBlockIds.isEmpty()) {
                    return Nxt.getGenesisBlockId();
                }
                // prevent overloading with blockIds
                if (milestoneBlockIds.size() > 20) {
                    Logger.logDebugMessage("Obsolete or rogue peer " + peer.getPeerAddress() + " sends too many milestoneBlockIds, blacklisting");
                    peer.blacklist();
                    return 0;
                }
                if (Boolean.TRUE.equals(response.get("last"))) {
                    peerHasMore = false;
                }
                for (Object milestoneBlockId : milestoneBlockIds) {
                    long blockId = Convert.parseUnsignedLong((String) milestoneBlockId);
                    if (BlockDb.hasBlock(blockId)) {
                        if (lastMilestoneBlockId == null && milestoneBlockIds.size() > 1) {
                            peerHasMore = false;
                        }
                        return blockId;
                    }
                    lastMilestoneBlockId = (String) milestoneBlockId;
                }
            }

        }

        private long getCommonBlockId(Peer peer, long commonBlockId) {

            while (true) {
                JSONObject request = new JSONObject();
                request.put("requestType", "getNextBlockIds");
                request.put("blockId", Convert.toUnsignedLong(commonBlockId));
                JSONObject response = peer.send(JSON.prepareRequest(request));
                if (response == null) {
                    return 0;
                }
                JSONArray nextBlockIds = (JSONArray) response.get("nextBlockIds");
            Logger.logDebugMessage("getNextBlockIds "+nextBlockIds.size());
                if (nextBlockIds == null || nextBlockIds.size() == 0) {
                    return 0;
                }
                /*
                // prevent overloading with blockIds
                if (nextBlockIds.size() > 1440) {
                    Logger.logDebugMessage("Obsolete or rogue peer " + peer.getPeerAddress() + " sends too many nextBlockIds, blacklisting");
                    peer.blacklist();
                    return 0;
                }*/

                for (Object nextBlockId : nextBlockIds) {
                    long blockId = Convert.parseUnsignedLong((String) nextBlockId);
                    if (! BlockDb.hasBlock(blockId)) {
                        return commonBlockId;
                    }
                    commonBlockId = blockId;
                }
            }

        }

        private JSONArray getNextBlocks(Peer peer, long curBlockId) {

            JSONObject request = new JSONObject();
            request.put("requestType", "getNextBlocks");
            request.put("blockId", Convert.toUnsignedLong(curBlockId));
            JSONObject response = peer.send(JSON.prepareRequest(request));
            if (response == null) {
                return null;
            }

            JSONArray nextBlocks = (JSONArray) response.get("nextBlocks");
            if (nextBlocks == null) {
                return null;
            }
            /*
            // prevent overloading with blocks
            if (nextBlocks.size() > 720) {
                Logger.logDebugMessage("Obsolete or rogue peer " + peer.getPeerAddress() + " sends too many nextBlocks, blacklisting");
                peer.blacklist();
                return null;
            }*/

            return nextBlocks;

        }

        private void downloadBlockchain(final Peer peer, final Block commonBlock) {
            JSONArray nextBlocks = getNextBlocks(peer, commonBlock.getId());
            if (nextBlocks == null || nextBlocks.size() == 0) {
                return;
            }

            List<BlockImpl> forkBlocks = new ArrayList<>();
            BlockImpl lastVerifiedBlock = (BlockImpl)commonBlock;

            for (Object o : nextBlocks) {
                JSONObject blockData = (JSONObject) o;
                BlockImpl block;
                try {
                    Logger.logDebugMessage("lastVerifiedBlock= "+lastVerifiedBlock.getId()+" "+lastVerifiedBlock.getHeight());
                    block = BlockPOW.parseBlock(blockData, lastVerifiedBlock);
                    lastVerifiedBlock = block; //parseBlock() encapsulates verify()
                    /*
                    if (block.verify(lastVerifiedBlock)){
                        lastVerifiedBlock = block;
                    }else{
                        throw new NxtException.NotValidException("blocks out of sequence");
                    }*/

                } catch (NxtException.NotCurrentlyValidException e) {
                    Logger.logDebugMessage("Cannot validate block: " + e.toString()
                            + ", will try again later", e);
                    break;
                } catch (RuntimeException | NxtException.ValidationException e) {
                    Logger.logDebugMessage("Failed to parse block: " + e.toString(), e);
                    peer.blacklist(e);
                    return;
                }

                /*
                if (blockchain.getLastBlock().getId() == block.getPreviousBlockId()) {
                    try {
                        pushBlock(block);
                        if (blockchain.getHeight() - commonBlock.getHeight() == 720 - 1) {
                            break;
                        }
                    } catch (BlockNotAcceptedException e) {
                        peer.blacklist(e);
                        return;
                    }
                } else {
                    forkBlocks.add(block);
                    if (forkBlocks.size() == 720 - 1) {
                        break;
                    }
                }*/

                forkBlocks.add(block);
            }

            if (forkBlocks.size() > 0 ) { //&& blockchain.getHeight() - commonBlock.getHeight() < 720
                //Logger.logDebugMessage("Will process a fork of " + forkBlocks.size() + " blocks");
                //processFork(peer, forkBlocks, commonBlock);
                Logger.logDebugMessage("Downloaded a chain of " + forkBlocks.size() + " blocks");
                if (lastVerifiedBlock.betterThan( blockchain.getLastBlock() )) {
                    Logger.logDebugMessage("Switching to peer's fork");
                    betterChainListener.notify(forkBlocks);
                }else{
                    Logger.logDebugMessage("Worse chain by peer " + peer.getPeerAddress() + ", blacklisting");
                    peer.blacklist();
                }
            }

        }

}

