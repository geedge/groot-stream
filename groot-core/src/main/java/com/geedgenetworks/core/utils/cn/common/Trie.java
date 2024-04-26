package com.geedgenetworks.core.utils.cn.common;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Trie tree
 *
 * @param <T> data type
 * @description Trie tree put every character of the string into a node, and the data is stored in the last node.
 * for example:
 * how to store:
 * if we put "baidu.com":"1" and "baidu.cn":"2" into the trie tree, the tree will be like this:
 * root -> b -> a -> i -> d -> u -> . -> c -> o -> m
 *                                         -> n
 * the data "1" is stored in the last node "m" and the data "2" is stored in the last node "n"
 * then we put "baidu":"3" into the trie tree, the tree will be like this:
 * root -> b -> a -> i -> d -> u -> . -> c -> o -> m
 *                                         -> n
 * the data "3" will be stored in the node "u"
 * <p>
 * how to get:
 * traversal the trie tree by the special string, and get all the data in the path
 * if we get "baidu.com" from the trie tree, we will get "1" and "3"
 * if we get "baidu.cn" from the trie tree, we will get "2" and "3"
 */
public class Trie<T> {

    private final Node<T> root = new Node<>();

    @Data
    private static class Node<T> {
        private char c;
        private List<T> data = new ArrayList<>();
        private Map<Character, Node<T>> childNode = new HashMap<>();
    }

    public void put(String index, T data) {
        List<T> list = new ArrayList<>();
        list.add(data);
        put(root, index, list);
    }

    public void putAll(String index, List<T> list) {
        put(root, index, list);
    }

    private void put(Node<T> node, String index, List<T> data) {
        if (index == null || index.length() == 0) {
            return;
        }
        boolean last = false;
        if (index.length() == 1) {
            last = true;
        }
        char c = index.charAt(0);
        if (node.getChildNode().containsKey(c)) {
            if (last) {
                node.getChildNode().get(c).getData().addAll(data);
            } else {
                put(node.getChildNode().get(c), index.substring(1), data);
            }
        } else {
            Node<T> newNode = new Node<T>();
            newNode.setC(c);
            if (last) {
                newNode.getData().addAll(data);
                node.getChildNode().put(c, newNode);
            } else {
                node.getChildNode().put(c, newNode);
                put(newNode, index.substring(1), data);
            }
        }
    }

    public List<T> get(String str) {
        List<T> result = new ArrayList<>();
        if (str == null || str.length() == 0) {
            return result;
        }
        get(root, str, result);
        return result;
    }

    private void get(Node<T> node, String str, List<T> result) {
        char c = str.charAt(0);
        if (node.getChildNode().containsKey(c)) {
            result.addAll(node.getChildNode().get(c).getData());
            if (str.length() > 1) {
                get(node.getChildNode().get(c), str.substring(1), result);
            }
        }
    }
}
