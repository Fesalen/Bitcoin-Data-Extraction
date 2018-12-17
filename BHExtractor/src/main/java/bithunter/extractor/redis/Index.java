package bithunter.extractor.redis;


import java.util.Map;
import java.util.TreeMap;

public class Index implements JO {
    private final String k;
    public final String v;

    Index(String k, String v) {
        this.k = k;
        this.v = v;
    }

    @Override
    public Map<String, String> describe() {
        return new TreeMap<String, String>() {{
            put("key", k);
            put("v", v);
        }};
    }

    @Override
    public String getKey() {
        return "IDX_" + k;
    }

    @Override
    public void saved(Class<? extends QueryBuilder> clazz, Exception e) {

    }

    static class SetIndex extends Index {
        SetIndex(String k, String v) {
            super(k, v);
        }
    }
}
