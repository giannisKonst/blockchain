package nxt;

import org.json.simple.JSONObject;

import java.math.BigInteger;
import java.util.List;

public interface BlockNXT extends Block {

    long getBaseTarget();
    byte[] getGeneratorPublicKey();
    byte[] getGenerationSignature();
    byte[] getBlockSignature();
    BigInteger getCumulativeDifficulty();

}
