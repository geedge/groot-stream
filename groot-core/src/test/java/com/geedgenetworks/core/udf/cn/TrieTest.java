package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.core.utils.cn.common.Trie;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TrieTest {

    @Test
    public void TrieGetTest() {
        Trie<String> trie = new Trie<>();
        trie.put(StringUtils.reverse("baidu.com"), "1");
        trie.put(StringUtils.reverse("test.baidu.com"), "2");
        trie.put(StringUtils.reverse("test1.baidu.com"), "3");
        trie.put(StringUtils.reverse("txj/r~/moc.elgoog.yxorpdeef//:ptth"), "4");

        List<String> strings1 = trie.get(StringUtils.reverse("test.baidu.com"));
        assertEquals(Arrays.asList("1", "2"), strings1);

        List<String> strings2 = trie.get(StringUtils.reverse("baidu.com"));
        assertEquals(Arrays.asList("1"), strings2);

        List<String> strings3 = trie.get(StringUtils.reverse("test.test.baidu.com"));
        assertEquals(Arrays.asList("1", "2"), strings3);

        List<String> strings4 = trie.get(StringUtils.reverse("test1.test.baidu.com"));
        assertEquals(Arrays.asList("1", "2"), strings4);

        List<String> strings5 = trie.get(StringUtils.reverse("test1.test1.baidu.com"));
        assertEquals(Arrays.asList("1", "3"), strings5);

        List<String> strings6 = trie.get(StringUtils.reverse(""));
        assertEquals(Arrays.asList(), strings6);

        List<String> strings7 = trie.get(StringUtils.reverse(null));
        assertEquals(Arrays.asList(), strings7);

        List<String> strings8 = trie.get(StringUtils.reverse("txj/r~/moc.elgoog.yxorpdeef//:ptth"));
        assertEquals(Arrays.asList("4"), strings8);
    }

    @Test
    public void testPutAndGet() {
        Trie<String> trie = new Trie<>();

        // Test putting a single data
        trie.put("apple", "fruit");
        assertEquals(Arrays.asList("fruit"), trie.get("apple"));

        // Test putting multiple data
        trie.putAll("banana", Arrays.asList("fruit", "yellow"));
        assertEquals(Arrays.asList("fruit", "yellow"), trie.get("banana"));

        // Test putting data with the same prefix
        trie.put("app", "application");
        trie.put("app", "appendix");
        assertEquals(Arrays.asList("application", "appendix"), trie.get("app"));

        // Test putting and getting with empty string
        trie.put("", "empty");
        assertEquals(Arrays.asList(), trie.get(""));
    }

    @Test
    public void testPutAll() {
        Trie<Integer> trie = new Trie<>();

        // Test putting multiple data at once
        trie.putAll("one", Arrays.asList(1, 11, 111));
        assertEquals(Arrays.asList(1, 11, 111), trie.get("one"));

        // Test putting an empty list
        trie.putAll("empty", Arrays.asList());
        assertEquals(Arrays.asList(), trie.get("empty"));
    }

    @Test
    public void testGet() {
        Trie<Double> trie = new Trie<>();

        // Test getting data with non-existent prefix
        assertEquals(Arrays.asList(), trie.get("nonexistent"));

        // Test getting data with partially matched prefix
        trie.put("test", 3.14);
        trie.put("testing", 2.71);
        assertEquals(Arrays.asList(), trie.get("tes"));

        // Test getting data with fully matched prefix
        assertEquals(Arrays.asList(3.14), trie.get("test"));

        // Test getting data with empty string
        trie.put("empty", 0.0);
        assertEquals(Arrays.asList(), trie.get(""));
    }
}
