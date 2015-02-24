package nxt;

import java.math.BigInteger;
import nxt.crypto.Crypto;

public final class Genesis {
    public static final String secret = "test";
    private static final byte[] publicKey = Crypto.getPublicKey(secret);

    public static final long CREATOR_ID = Account.getId(publicKey);
    public static final byte[] CREATOR_PUBLIC_KEY = publicKey;
    //public static final long GENESIS_BLOCK_ID = 2680262203532249785L;

    public static final long[] GENESIS_RECIPIENTS = {
            Account.getId(Crypto.getPublicKey("test1")),
            Account.getId(Crypto.getPublicKey("test2"))
    };

    public static final int[] GENESIS_AMOUNTS = {
            500,
            501
    };

    public static final byte[][] GENESIS_SIGNATURES = {
            {28, -48, 70, -35, 123, -31, 16, -52, 72, 84, -51, 78, 104, 59, -102, -112, 29, 28, 25, 66, 12, 75, 26, -85, 56, -12, -4, -92, 49, 86, -27, 12, 44, -63, 108, 82, -76, -97, -41, 95, -48, -95, -115, 1, 64, -49, -97, 90, 65, 46, -114, -127, -92, 79, 100, 49, 116, -58, -106, 9, 117, -7, -91, 96},
            {58, 26, 18, 76, 127, -77, -58, -87, -116, -44, 60, -32, -4, -76, -124, 4, -60, 82, -5, -100, -95, 18, 2, -53, -50, -96, -126, 105, 93, 19, 74, 13, 87, 125, -72, -10, 42, 14, 91, 44, 78, 52, 60, -59, -27, -37, -57, 17, -85, 31, -46, 113, 100, -117, 15, 108, -42, 12, 47, 63, 1, 11, -122, -3}
    };

    public static final byte[] GENESIS_BLOCK_SIGNATURE = new byte[]{
            105, -44, 38, -60, -104, -73, 10, -58, -47, 103, -127, -128, 53, 101, 39, -63, -2, -32, 48, -83, 115, 47, -65, 118, 114, -62, 38, 109, 22, 106, 76, 8, -49, -113, -34, -76, 82, 79, -47, -76, -106, -69, -54, -85, 3, -6, 110, 103, 118, 15, 109, -92, 82, 37, 20, 2, 36, -112, 21, 72, 108, 72, 114, 17
    };

    private Genesis() {} // never

}
