package com.geedgenetworks.core.utils;

import com.geedgenetworks.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.http.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
@Slf4j
public class HttpClientPoolUtil {

    private static HttpClientPoolUtil instance;
    private CloseableHttpClient httpClient;

    private HttpClientPoolUtil() {
        // 创建连接池管理器
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        // 设置最大连接数
        connectionManager.setMaxTotal(400);
        // 设置每个路由的最大连接数
        connectionManager.setDefaultMaxPerRoute(60);
        // 创建 HttpClient 实例
        httpClient = HttpClientBuilder.create()
                .setConnectionManager(connectionManager)
                .build();
    }

    public static synchronized HttpClientPoolUtil getInstance() {
        if (instance == null) {
            instance = new HttpClientPoolUtil();
        }
        return instance;
    }

    public String httpGet(URI uri, Header... headers) throws IOException {
        HttpGet httpGet = new HttpGet(uri);
        String msg ="";
        if (StringUtil.isNotEmpty(headers)) {
            for (Header h : headers) {
                httpGet.addHeader(h);
                log.info("request header : {}", h);
            }
        }
        try(CloseableHttpResponse response = httpClient.execute(httpGet)) {
            int statusCode = response.getStatusLine().getStatusCode();
            // 获取响应实体
            HttpEntity entity = response.getEntity();
            // 获取响应信息
            msg = EntityUtils.toString(entity, "UTF-8");

            if (statusCode != HttpStatus.SC_OK) {
                log.error("Http get content is :{}", msg);
            }
        }
        return msg;
    }

    public void close() throws IOException {
        httpClient.close();
    }

    public byte[] httpGetByte(String url, Header... headers) {
        byte[] result = null;
        // 获取客户端连接对象
        // 创建GET请求对象
        HttpGet httpGet = new HttpGet(url);
        if (StringUtil.isNotEmpty(headers)) {
            for (Header h : headers) {
                httpGet.addHeader(h);
            }
        }

        try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
            int statusCode = response.getStatusLine().getStatusCode();
            // 获取响应实体
            result = IOUtils.toByteArray(response.getEntity().getContent());

            if (statusCode != HttpStatus.SC_OK) {
                log.error("httpGetByte error ! " + statusCode);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;

    }

}
