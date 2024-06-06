package com.geedgenetworks.connectors.mock.faker;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class UnionFakerTest {

    public static void main(String[] args) throws Exception{
        //testWeightRandom();
        testUnionFaker();
    }

    public static void testWeightRandom(){
        Random r = new Random();
        int[] items = new int[]{3, 1, 2, 6};
        int[] weights = new int[items.length];
        weights[0] = items[0];
        for (int i = 1; i < weights.length; i++) {
            weights[i] = items[i] + weights[i - 1];
        }
        int weightMax = weights[weights.length - 1];
        System.out.println(Arrays.toString(items));
        System.out.println(Arrays.toString(weights));
        System.out.println("weightMax:" + weightMax);

        Map<Integer, Integer> itemCounts = new HashMap<>();
        for (int i = 0; i < 1000000; i++) {
            int key = r.nextInt(weightMax) + 1;
            int index = Arrays.binarySearch(weights, key);
            if (index < 0) {
                index = -index - 1;
            }
            itemCounts.put(items[index], itemCounts.getOrDefault(items[index], 0) + 1);
        }

        System.out.println(itemCounts);
    }

    public static void testUnionFaker() throws Exception{
        String json = "[  {\n" +
                "    \"name\": \"random_int\",\n" +
                "    \"type\": \"Number\",\n" +
                "    \"start\": 0,\n" +
                "    \"end\": 10000\n" +
                "  },  {\n" +
                "    \"name\": \"unionFields\",\n" +
                "    \"type\": \"Union\",\n" +
                "    \"random\": false,\n" +
                "    \"unionFields\": [\n" +
                "      {\n" +
                "        \"weight\": 5,\n" +
                "        \"fields\": [\n" +
                "          {\n" +
                "            \"name\": \"object_id\",\n" +
                "            \"type\": \"Number\",\n" +
                "            \"options\": [10]\n" +
                "          },\n" +
                "          {\n" +
                "            \"name\": \"item_id\",\n" +
                "            \"type\": \"Number\",\n" +
                "            \"options\": [1, 2, 3, 4, 5],\n" +
                "            \"random\": false\n" +
                "          }\n" +
                "        ]\n" +
                "      },\n" +
                "      {\n" +
                "        \"weight\": 2,\n" +
                "        \"fields\": [\n" +
                "          {\n" +
                "            \"name\": \"object_id\",\n" +
                "            \"type\": \"Number\",\n" +
                "            \"options\": [20]\n" +
                "          },\n" +
                "          {\n" +
                "            \"name\": \"item_id\",\n" +
                "            \"type\": \"Number\",\n" +
                "            \"options\": [6, 7],\n" +
                "            \"random\": false\n" +
                "          }\n" +
                "        ]\n" +
                "      }\n" +
                "    ]\n" +
                "  }]";

        ObjectFaker objectFaker = FakerUtils.parseObjectFakerFromJson(json);
        objectFaker.init(1, 0);
        for (int i = 0; i < 20; i++) {
            System.out.println(objectFaker.geneValue());
        }
        objectFaker.destroy();
    }

}