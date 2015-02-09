package nxt;

import org.json.simple.JSONObject;

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

import nxt.Transaction;
import nxt.TransactionImpl;
import java.util.Comparator;

public class BlockNXTImpl extends BlockImpl implements BlockNXT {

    private final int version;
    private final byte[] generationSignature;
    private byte[] blockSignature;
    private BigInteger cumulativeDifficulty = BigInteger.ZERO;
    private long baseTarget = Constants.INITIAL_BASE_TARGET;

    private final byte[] generatorPublicKey;
    private volatile long generatorId;

    private BlockNXTImpl(int version, int timestamp, long previousBlockId, long totalAmountNQT, long totalFeeNQT, int payloadLength, byte[] payloadHash,
              byte[] generatorPublicKey, byte[] generationSignature, byte[] blockSignature, byte[] previousBlockHash, List<TransactionImpl> transactions)
            throws NxtException.ValidationException {
	super(timestamp, previousBlockId, totalAmountNQT, totalFeeNQT, payloadLength, payloadHash, previousBlockHash, transactions);

        this.version = 3;
        this.generatorPublicKey = generatorPublicKey;
        this.generationSignature = generationSignature;
        this.blockSignature = blockSignature;
    }

    BlockNXTImpl(int version, int timestamp, long previousBlockId, long totalAmountNQT, long totalFeeNQT, int payloadLength,
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

    BlockNXTImpl(int timestamp, Block previousBlock1, byte[] publicKey, List<TransactionImpl> transactions) throws NxtException.ValidationException {
	/* super(version, timestamp, previousBlockId, totalAmountNQT, totalFeeNQT, payloadLength, payloadHash,
              previousBlockHash, List<TransactionImpl> transactions); */
	super(timestamp, previousBlock1, transactions);

	BlockNXTImpl previousBlock = (BlockNXTImpl) previousBlock1;

        MessageDigest digest = Crypto.sha256();
        digest.update(previousBlock.getGenerationSignature());
        byte[] generationSignature = digest.digest(publicKey);

        byte[] previousBlockHash = previousBlock.getHash();

        this.version = 3;
        this.generatorPublicKey = publicKey;
        this.generationSignature = generationSignature;
        this.blockSignature = blockSignature;
    }

    @Override
    public byte[] getHash() {
        return Crypto.sha256().digest(this.getBytes());
    }

    @Override
    public long getId() {
        if (blockSignature == null) {
	    throw new IllegalStateException("Block is not signed yet");
        }
        return super.getId();
    }

    @Override
    public long getBaseTarget() {
        return baseTarget;
    }

    @Override
    public BigInteger getCumulativeDifficulty() {
        return cumulativeDifficulty;
    }

    @Override
    public byte[] getGeneratorPublicKey() {
        return generatorPublicKey;
    }

    @Override
    public byte[] getGenerationSignature() {
        return generationSignature;
    }

    @Override
    public byte[] getBlockSignature() {
        return blockSignature;
    }

    @Override
    public JSONObject getJSONObject() { //for Peers
        JSONObject json = super.getJSONObject();
        json.put("version", version);
        json.put("generatorPublicKey", Convert.toHexString(generatorPublicKey));
        json.put("generationSignature", Convert.toHexString(generationSignature));
        json.put("blockSignature", Convert.toHexString(blockSignature));
        return json;
    }

    /*
    public JSONObject getJSONObject() {
        this.getJSONObject(false);
    }*/

   static BlockNXTImpl parseBlock(JSONObject blockData) throws NxtException.ValidationException {
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

    public byte[] getBytes() {
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

    boolean verifyGenerationSignature() {

        try {

            BlockNXTImpl previousBlock = (BlockNXTImpl) BlockchainImpl.getInstance().getBlock(getPreviousBlockId());
            if (previousBlock == null) {
                throw new RuntimeException("Can't verify signature because previous block is missing");
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

    @Override
    void setPrevious(Block block1) {
        super.setPrevious(block1);
        BlockNXTImpl block = (BlockNXTImpl) block1;
        if (block != null) {
            this.calculateBaseTarget(block);
        }
    }

    private void calculateBaseTarget(BlockNXTImpl previousBlock) {

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
 
    public long getGeneratorId() {
        if (generatorId == 0) {
            generatorId = Account.getId(generatorPublicKey);
        }
        return generatorId;
    }

    boolean verify() {
        /*
        if (!verifyGenerationSignature()) { // && !Generator.allowsFakeForging(block.getGeneratorPublicKey())
            throw new BlockchainProcessor.BlockNotAcceptedException("Generation signature verification failed");
        }
        if (!verifyBlockSignature()) {
            throw new BlockchainProcessor.BlockNotAcceptedException("Block signature verification failed");
        }*/
	/*
        if (currentBlock.getVersion() != getBlockVersion(blockchain.getHeight())) {
            throw new NxtException.NotValidException("Invalid block version");
        }*/
        return verifyGenerationSignature() && verifyBlockSignature();
    }

    @Override
    public int getVersion() {
        return version;
    }
 
    @Override
    void apply() throws BlockchainProcessor.TransactionNotAcceptedException {
	super.apply();
        Account generatorAccount = Account.addOrGetAccount(getGeneratorId());
        generatorAccount.apply(generatorPublicKey, this.height);
        generatorAccount.addToBalanceAndUnconfirmedBalanceNQT(totalFeeNQT);
        generatorAccount.addToForgedBalanceNQT(totalFeeNQT);
    }

    public static Block getGenesisBlock() {
        try {
	    List<TransactionImpl> transactions = new ArrayList<>();
	    for (int i = 0; i < Genesis.GENESIS_RECIPIENTS.length; i++) {
		TransactionImpl transaction = new TransactionImpl.BuilderImpl((byte) 0, Genesis.CREATOR_PUBLIC_KEY,
			Genesis.GENESIS_AMOUNTS[i] * Constants.ONE_NXT, 0, (short) 0,
			Attachment.ORDINARY_PAYMENT)
			.timestamp(0)
			.recipientId(Genesis.GENESIS_RECIPIENTS[i])
			.signature(Genesis.GENESIS_SIGNATURES[i])
			.height(0)
			.ecBlockHeight(0)
			.ecBlockId(0)
			.build();
		transactions.add(transaction);
	    }
	    Collections.sort(transactions, new Comparator<TransactionImpl>() {
		@Override
		public int compare(TransactionImpl o1, TransactionImpl o2) {
		    return Long.compare(o1.getId(), o2.getId());
		}
	    });
	    MessageDigest digest = Crypto.sha256();
	    for (Transaction transaction : transactions) {
		digest.update(transaction.getBytes());
	    }
	    return new BlockNXTImpl(-1, 0, 0, Constants.MAX_BALANCE_NQT, 0, transactions.size() * 128, digest.digest(),
                    Genesis.CREATOR_PUBLIC_KEY, new byte[64], Genesis.GENESIS_BLOCK_SIGNATURE, null, transactions);
        }catch(Exception e){
            throw new Error("GenesisBlock");
        }
    }
}
