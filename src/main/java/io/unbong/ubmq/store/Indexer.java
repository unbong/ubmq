package io.unbong.ubmq.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * entry indexer
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-14 16:35
 */
public class Indexer {

    // key: topic, value: Entry List
    static MultiValueMap<String, Entry> indecies = new LinkedMultiValueMap<>();

    // key: offset, value:Entry
    static  Map<Integer, Entry> mappings = new HashMap<>();

    @Data
    @AllArgsConstructor
    public static class Entry{
//        long id; // message id
        int offset; // offset in memory
        int length; // current data length
    }

    public static void addEntry(String topic , int offset, int length){
        Entry entry = new Entry( offset,length);
        indecies.add(topic, entry);
        mappings.put(offset, entry);
    }

    public  static List<Entry> getEntry(String topic){
        return indecies.get(topic);
    }

    public static Entry getEntry(String topic, int offset){
        return mappings.get(offset);
    }


}
