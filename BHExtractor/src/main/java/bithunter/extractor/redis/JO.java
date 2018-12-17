package bithunter.extractor.redis;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface JO {
    Map<String, String> describe();
    Object getKey();
    default List<Index> getIndex() {
        return Collections.emptyList();
    }
    void saved(Class<? extends QueryBuilder> clazz, Exception e);
}
