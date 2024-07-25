package com.geedgenetworks.core.utils;

import com.geedgenetworks.shaded.org.apache.http.Header;
import com.geedgenetworks.shaded.org.apache.http.HttpEntity;
import com.geedgenetworks.shaded.org.apache.http.HttpStatus;
import com.geedgenetworks.shaded.org.apache.http.client.methods.CloseableHttpResponse;
import com.geedgenetworks.shaded.org.apache.http.client.methods.HttpGet;
import com.geedgenetworks.shaded.org.apache.http.config.Registry;
import com.geedgenetworks.shaded.org.apache.http.config.RegistryBuilder;
import com.geedgenetworks.shaded.org.apache.http.conn.socket.ConnectionSocketFactory;
import com.geedgenetworks.shaded.org.apache.http.conn.socket.PlainConnectionSocketFactory;
import com.geedgenetworks.shaded.org.apache.http.conn.ssl.NoopHostnameVerifier;
import com.geedgenetworks.shaded.org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import com.geedgenetworks.shaded.org.apache.http.impl.client.CloseableHttpClient;
import com.geedgenetworks.shaded.org.apache.http.impl.client.HttpClientBuilder;
import com.geedgenetworks.shaded.org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import com.geedgenetworks.shaded.org.apache.http.util.EntityUtils;
import com.geedgenetworks.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

@Slf4j
public class HttpClientPoolUtil {

    private static HttpClientPoolUtil instance;
    private final CloseableHttpClient httpClient;

    private static final int  DEFAULT_MAX_TOTAL = 400;
    private static final int  DEFAULT_MAX_PER_ROUTE = 10;
    private static final int DEFAULT_CONNECTION_REQUEST_TIMEOUT = 10000;
    private static final int DEFAULT_CONNECT_TIMEOUT = 10000;
    private static final int DEFAULT_SOCKET_TIMEOUT = 60000;
    private HttpClientPoolUtil() {
        // 创建连接池管理器
        PoolingHttpClientConnectionManager connectionManager = getSslClientManager();
        // 设置最大连接数
        connectionManager.setMaxTotal(DEFAULT_MAX_TOTAL);
        // 设置每个路由的最大连接数
        connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_PER_ROUTE);
        // 创建 HttpClient 实例
        httpClient = HttpClientBuilder.create()
                .setConnectionManager(connectionManager)
                .build();
    }

    private PoolingHttpClientConnectionManager getSslClientManager() {
        try {
            // 在调用SSL之前需要重写验证方法，取消检测SSL
            X509TrustManager trustManager =
                    new X509TrustManager() {
                        @Override
                        public X509Certificate[] getAcceptedIssuers() {
                            return null;
                        }

                        @Override
                        public void checkClientTrusted(X509Certificate[] xcs, String str) {}

                        @Override
                        public void checkServerTrusted(X509Certificate[] xcs, String str) {}
                    };
            SSLContext ctx = SSLContext.getInstance(SSLConnectionSocketFactory.TLS);
            ctx.init(null, new TrustManager[] {trustManager}, null);
            SSLConnectionSocketFactory socketFactory =
                    new SSLConnectionSocketFactory(ctx, NoopHostnameVerifier.INSTANCE);
            Registry<ConnectionSocketFactory> socketFactoryRegistry =
                    RegistryBuilder.<ConnectionSocketFactory>create()
                            .register("http", PlainConnectionSocketFactory.INSTANCE)
                            .register("https", socketFactory)
                            .build();
            // 创建ConnectionManager，添加Connection配置信息
            PoolingHttpClientConnectionManager connManager =
                    new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            // 设置最大连接数
            connManager.setMaxTotal(DEFAULT_MAX_TOTAL);
            // 设置每个连接的路由数
            connManager.setDefaultMaxPerRoute(DEFAULT_MAX_PER_ROUTE);
            return connManager;
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
        }
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
                // log.info("request header : {}", h);
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
