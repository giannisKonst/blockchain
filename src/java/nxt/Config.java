package nxt;

import org.json.simple.JSONObject;

public final class Config {

    public static final Class<? extends BlockImpl> blockClass = BlockBA.class;
    public static final Class<? extends Generator> generatorClass = GeneratorAgreement.class;

    public static class BlockFactory {

        public static BlockImpl genesis() {
            return (BlockImpl) BlockBA.getGenesisBlock();
        }

        public static BlockImpl parseBlock(JSONObject json, BlockImpl previousVerifiedBlock) throws NxtException.ValidationException {
            BlockImpl block = new BlockBA(json, previousVerifiedBlock);
            block.verify();
            return block;
        }

        public static BlockImpl parseBlock(JSONObject json) throws NxtException.ValidationException {
            return parseBlock(json, null);
        }

        //public BlockImpl loadFromDB(ResultSet?) //TODO
    }
    

    private Config(){}
}
