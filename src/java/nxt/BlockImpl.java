package nxt;

import nxt.crypto.Crypto;
import nxt.util.Convert;
import nxt.util.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

abstract class BlockImpl implements Block {

    private final int version;
    private final int timestamp;
    private final long previousBlockId;
    private final byte[] previousBlockHash;
    private final long totalAmountNQT;
    private final long totalFeeNQT;
    private volatile List<TransactionImpl> blockTransactions;
    private final int payloadLength;
    private final byte[] payloadHash;

    private final byte[] generatorPublicKey;
    private volatile long generatorId;

    private volatile long nextBlockId;
    private int height = -1;
    private volatile long id;
    private volatile String stringId = null;

    BlockImpl(int version, int timestamp, long previousBlockId, long totalAmountNQT, long totalFeeNQT, int payloadLength, byte[] payloadHash,
              byte[] generatorPublicKey, byte[] generationSignature, byte[] blockSignature, byte[] previousBlockHash, List<TransactionImpl> transactions)
            throws NxtException.ValidationException {

        if (payloadLength > Constants.MAX_PAYLOAD_LENGTH || payloadLength < 0) {
            throw new NxtException.NotValidException("attempted to create a block with payloadLength " + payloadLength);
        }

        this.version = version;
        this.timestamp = timestamp;
        this.previousBlockId = previousBlockId;
        this.totalAmountNQT = totalAmountNQT;
        this.totalFeeNQT = totalFeeNQT;
        this.payloadLength = payloadLength;
        this.payloadHash = payloadHash;
        //this.generatorPublicKey = generatorPublicKey;
        //this.generationSignature = generationSignature;
        //this.blockSignature = blockSignature;
        this.previousBlockHash = previousBlockHash;
        if (transactions != null) {
            this.blockTransactions = Collections.unmodifiableList(transactions);
            if (blockTransactions.size() > Constants.MAX_NUMBER_OF_TRANSACTIONS) {
                throw new NxtException.NotValidException("attempted to create a block with " + blockTransactions.size() + " transactions");
            }
        }

    }

/*
    BlockImpl(int version, int timestamp, long previousBlockId, long totalAmountNQT, long totalFeeNQT, int payloadLength,
              byte[] payloadHash, byte[] generatorPublicKey, byte[] generationSignature, byte[] blockSignature,
              byte[] previousBlockHash, BigInteger cumulativeDifficulty, long baseTarget, long nextBlockId, int height, long id)
            throws NxtException.ValidationException {
        this(version, timestamp, previousBlockId, totalAmountNQT, totalFeeNQT, payloadLength, payloadHash,
                generatorPublicKey, generationSignature, blockSignature, previousBlockHash, null);
        this.cumulativeDifficulty = cumulativeDifficulty;
        this.baseTarget = baseTarget;
        this.nextBlockId = nextBlockId;
        this.height = height;
        this.id = id;
    }
*/

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public int getTimestamp() {
        return timestamp;
    }

    @Override
    public long getPreviousBlockId() {
        return previousBlockId;
    }

    @Override
    public byte[] getPreviousBlockHash() {
        return previousBlockHash;
    }

    @Override
    public long getTotalAmountNQT() {
        return totalAmountNQT;
    }

    @Override
    public long getTotalFeeNQT() {
        return totalFeeNQT;
    }

    @Override
    public int getPayloadLength() {
        return payloadLength;
    }

    @Override
    public byte[] getPayloadHash() {
        return payloadHash;
    }

    @Override
    public List<TransactionImpl> getTransactions() {
        if (blockTransactions == null) {
            this.blockTransactions = Collections.unmodifiableList(TransactionDb.findBlockTransactions(getId()));
            for (TransactionImpl transaction : this.blockTransactions) {
                transaction.setBlock(this);
            }
        }
        return blockTransactions;
    }


    @Override
    public long getNextBlockId() {
        return nextBlockId;
    }

    @Override
    public int getHeight() {
        if (height == -1) {
            throw new IllegalStateException("Block height not yet set");
        }
        return height;
    }

    @Override
    abstract public long getId();

    @Override
    public String getStringId() {
        if (stringId == null) {
            getId();
            if (stringId == null) {
                stringId = Convert.toUnsignedLong(id);
            }
        }
        return stringId;
    }

    @Override
    public long getGeneratorId() {
        if (generatorId == 0) {
            generatorId = Account.getId(generatorPublicKey);
        }
        return generatorId;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof BlockImpl && this.getId() == ((BlockImpl)o).getId();
    }

    @Override
    public int hashCode() {
        return (int)(getId() ^ (getId() >>> 32));
    }

    @Override
    abstract public JSONObject getJSONObject();

    static BlockImpl parseBlock(JSONObject blockData) throws NxtException.ValidationException {
        try {
            int version = ((Long) blockData.get("version")).intValue();
            int timestamp = ((Long) blockData.get("timestamp")).intValue();
            long previousBlock = Convert.parseUnsignedLong((String) blockData.get("previousBlock"));
            long totalAmountNQT = Convert.parseLong(blockData.get("totalAmountNQT"));
            long totalFeeNQT = Convert.parseLong(blockData.get("totalFeeNQT"));
            int payloadLength = ((Long) blockData.get("payloadLength")).intValue();
            byte[] payloadHash = Convert.parseHexString((String) blockData.get("payloadHash"));
            byte[] previousBlockHash = version == 1 ? null : Convert.parseHexString((String) blockData.get("previousBlockHash"));
            List<TransactionImpl> blockTransactions = new ArrayList<>();
            for (Object transactionData : (JSONArray) blockData.get("transactions")) {
                blockTransactions.add(TransactionImpl.parseTransaction((JSONObject) transactionData));
            }

            byte[] generatorPublicKey = Convert.parseHexString((String) blockData.get("generatorPublicKey"));
            byte[] generationSignature = Convert.parseHexString((String) blockData.get("generationSignature"));
            byte[] blockSignature = Convert.parseHexString((String) blockData.get("blockSignature"));

            return new BlockNXTImpl(version, timestamp, previousBlock, totalAmountNQT, totalFeeNQT, payloadLength, payloadHash, generatorPublicKey,
                    generationSignature, blockSignature, previousBlockHash, blockTransactions);
        } catch (NxtException.ValidationException|RuntimeException e) {
            Logger.logDebugMessage("Failed to parse block: " + blockData.toJSONString());
            throw e;
        }
    }

    abstract byte[] getBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 8 + 4 + (version < 3 ? (4 + 4) : (8 + 8)) + 4 + 32 + 32 + (32 + 32) + 64);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(version);
        buffer.putInt(timestamp);
        buffer.putLong(previousBlockId);
        buffer.putInt(getTransactions().size());
        if (version < 3) {
            buffer.putInt((int)(totalAmountNQT / Constants.ONE_NXT));
            buffer.putInt((int)(totalFeeNQT / Constants.ONE_NXT));
        } else {
            buffer.putLong(totalAmountNQT);
            buffer.putLong(totalFeeNQT);
        }
        buffer.putInt(payloadLength);
        buffer.put(payloadHash);
        buffer.put(generatorPublicKey);
        buffer.put(generationSignature);
        if (version > 1) {
            buffer.put(previousBlockHash);
        }
        buffer.put(blockSignature);
        return buffer.array();
    }

    void sign(String secretPhrase) {
        if (blockSignature != null) {
            throw new IllegalStateException("Block already signed");
        }
        blockSignature = new byte[64];
        byte[] data = getBytes();
        byte[] data2 = new byte[data.length - 64];
        System.arraycopy(data, 0, data2, 0, data2.length);
        blockSignature = Crypto.sign(data2, secretPhrase);
    }

    boolean verifyBlockSignature() {

        Account account = Account.getAccount(getGeneratorId());
        if (account == null) {
            return false;
        }

        byte[] data = getBytes();
        byte[] data2 = new byte[data.length - 64];
        System.arraycopy(data, 0, data2, 0, data2.length);

        return Crypto.verify(blockSignature, data2, generatorPublicKey, version >= 3) && account.setOrVerify(generatorPublicKey, this.height);

    }

    boolean verifyGenerationSignature() throws BlockchainProcessor.BlockOutOfOrderException {

        try {

            BlockImpl previousBlock = BlockchainImpl.getInstance().getBlock(getPreviousBlockId());
            if (previousBlock == null) {
                throw new BlockchainProcessor.BlockOutOfOrderException("Can't verify signature because previous block is missing");
            }

            if (version == 1 && !Crypto.verify(generationSignature, previousBlock.generationSignature, generatorPublicKey, version >= 3)) {
                return false;
            }

            Account account = Account.getAccount(getGeneratorId());
            long effectiveBalance = account == null ? 0 : account.getEffectiveBalanceNXT();
            if (effectiveBalance <= 0) {
                return false;
            }

            MessageDigest digest = Crypto.sha256();
            byte[] generationSignatureHash;
            if (version == 1) {
                generationSignatureHash = digest.digest(generationSignature);
            } else {
                digest.update(previousBlock.generationSignature);
                generationSignatureHash = digest.digest(generatorPublicKey);
                if (!Arrays.equals(generationSignature, generationSignatureHash)) {
                    return false;
                }
            }

            BigInteger hit = new BigInteger(1, new byte[]{generationSignatureHash[7], generationSignatureHash[6], generationSignatureHash[5], generationSignatureHash[4], generationSignatureHash[3], generationSignatureHash[2], generationSignatureHash[1], generationSignatureHash[0]});

            return ProofOfNXT.verifyHit(hit, BigInteger.valueOf(effectiveBalance), previousBlock, timestamp);

        } catch (RuntimeException e) {

            Logger.logMessage("Error verifying block generation signature", e);
            return false;

        }

    }

    void apply() throws BlockchainProcessor.TransactionNotAcceptedException {
        Account generatorAccount = Account.addOrGetAccount(getGeneratorId());
        generatorAccount.apply(generatorPublicKey, this.height);
        generatorAccount.addToBalanceAndUnconfirmedBalanceNQT(totalFeeNQT);
        generatorAccount.addToForgedBalanceNQT(totalFeeNQT);
        for (TransactionImpl transaction : getTransactions()) {
            try {
                transaction.apply();
            } catch (RuntimeException e) {
                Logger.logErrorMessage(e.toString(), e);
                throw new BlockchainProcessor.TransactionNotAcceptedException(e, transaction);
            }
        }
    }

    void setPrevious(BlockImpl block) {
        if (block != null) {
            if (block.getId() != getPreviousBlockId()) {
                // shouldn't happen as previous id is already verified, but just in case
                throw new IllegalStateException("Previous block id doesn't match");
            }
            this.height = block.getHeight() + 1;
            this.calculateBaseTarget(block);
        } else {
            this.height = 0;
        }
        short index = 0;
        for (TransactionImpl transaction : getTransactions()) {
            transaction.setBlock(this);
            transaction.setIndex(index++);
        }
    }

    private void calculateBaseTarget(BlockImpl previousBlock) {

        if (this.getId() != Genesis.GENESIS_BLOCK_ID || previousBlockId != 0) {
            long curBaseTarget = previousBlock.baseTarget;
            long newBaseTarget = BigInteger.valueOf(curBaseTarget)
                    .multiply(BigInteger.valueOf(this.timestamp - previousBlock.timestamp))
                    .divide(BigInteger.valueOf(60)).longValue();
            if (newBaseTarget < 0 || newBaseTarget > Constants.MAX_BASE_TARGET) {
                newBaseTarget = Constants.MAX_BASE_TARGET;
            }
            if (newBaseTarget < curBaseTarget / 2) {
                newBaseTarget = curBaseTarget / 2;
            }
            if (newBaseTarget == 0) {
                newBaseTarget = 1;
            }
            long twofoldCurBaseTarget = curBaseTarget * 2;
            if (twofoldCurBaseTarget < 0) {
                twofoldCurBaseTarget = Constants.MAX_BASE_TARGET;
            }
            if (newBaseTarget > twofoldCurBaseTarget) {
                newBaseTarget = twofoldCurBaseTarget;
            }
            baseTarget = newBaseTarget;
            cumulativeDifficulty = previousBlock.cumulativeDifficulty.add(Convert.two64.divide(BigInteger.valueOf(baseTarget)));
        }
    }

}
