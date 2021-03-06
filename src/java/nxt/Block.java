package nxt;

import org.json.simple.JSONObject;

import java.math.BigInteger;
import java.util.List;

public interface Block {

    int getVersion();

    long getId();

    String getStringId();

    int getHeight();

    int getTimestamp();

    long getPreviousBlockId();

    byte[] getPreviousBlockHash();

    long getNextBlockId();

    long getTotalAmountNQT();

    long getTotalFeeNQT();

    int getPayloadLength();

    byte[] getPayloadHash();

    byte[] getHash();

    List<? extends Transaction> getTransactions();

    //Proof getProofOfWinning();
    //long getBaseTarget();  //rmv
    //BigInteger getCumulativeDifficulty();  //rmv

    JSONObject getJSONObject(); //for Peers

    //JSONObject getJSONObject(boolean includeTransactions); //for http

    boolean betterThan(Block chainHead);

/*
*/
    //TODO REMOVE
    long getBaseTarget();
    BigInteger getCumulativeDifficulty();
//    byte[] getBlockSignature();
    long getGeneratorId();
//    byte[] getGenerationSignature();
//    byte[] getGeneratorPublicKey();
}

