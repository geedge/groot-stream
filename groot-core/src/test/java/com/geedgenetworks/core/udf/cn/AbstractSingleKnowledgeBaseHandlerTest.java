package com.geedgenetworks.core.udf.cn;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.core.pojo.KnowLedgeBaseFileMeta;
import com.geedgenetworks.core.udf.knowlegdebase.handler.AbstractSingleKnowledgeBaseHandler;
import com.geedgenetworks.crypt.AESUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AbstractSingleKnowledgeBaseHandlerTest {

    private static KnowledgeBaseConfig knowledgeBaseConfig;

    private ClientAndServer mockGatewayServer;

    private MockServerClient mockHosServer;

    @BeforeEach
    void beforeEach() {
        knowledgeBaseConfig = new KnowledgeBaseConfig();
        knowledgeBaseConfig.setFsPath("http://localhost:9999/v1/knowledge_base");
        knowledgeBaseConfig.setFsType("http");
        knowledgeBaseConfig.setFiles(List.of("1"));
    }

    @Test
    void downloadCsvFile() {
        KnowLedgeBaseFileMeta knowLedgeBaseFileMeta = new KnowLedgeBaseFileMeta();
        knowLedgeBaseFileMeta.setPath("http://localhost:9098/hos/knowledge_base_bucket/1_latest");
        knowLedgeBaseFileMeta.setIsValid(1);
        knowLedgeBaseFileMeta.setFormat("csv");
        knowLedgeBaseFileMeta.setVersion("latest");
        Map<String, List<KnowLedgeBaseFileMeta>> gatewayResponse = new HashMap<>();
        gatewayResponse.put("data", List.of(knowLedgeBaseFileMeta));

        mockGatewayServer = ClientAndServer.startClientAndServer(9999);
        MockServerClient gatewayClient = new MockServerClient("localhost", 9999);

        // 定义 MockServer 的行为
        gatewayClient.when(
                HttpRequest.request()
                        .withMethod("GET")
                        .withPath("/v1/knowledge_base")
                        .withQueryStringParameter("kb_id", "1")
        ).respond(
                HttpResponse.response()
                        .withStatusCode(200)
                        .withBody(JSON.toJSONString(gatewayResponse))
        );

        mockHosServer = ClientAndServer.startClientAndServer(9098);
        MockServerClient hosClient = new MockServerClient("localhost", 9098);

        // 定义 MockServer 的行为
        hosClient.when(
                HttpRequest.request()
                        .withMethod("GET")
                        .withPath("/hos/knowledge_base_bucket/1_latest")
        ).respond(
                HttpResponse.response()
                        .withStatusCode(200)
                        .withBody("test")
        );

        AbstractSingleKnowledgeBaseHandler baseHandler = new AbstractSingleKnowledgeBaseHandler() {
            @Override
            protected Boolean buildKnowledgeBase() {
                byte[] bytes = downloadFile();
                assertEquals("test", new String(bytes));
                return true;
            }

            @Override
            public void close() {

            }
        };
        baseHandler.initKnowledgeBase(knowledgeBaseConfig);
    }

    @Test
    void downloadAesFile() throws Exception {
        KnowLedgeBaseFileMeta knowLedgeBaseFileMeta = new KnowLedgeBaseFileMeta();
        knowLedgeBaseFileMeta.setPath("http://localhost:9098/hos/knowledge_base_bucket/1_latest");
        knowLedgeBaseFileMeta.setIsValid(1);
        knowLedgeBaseFileMeta.setFormat("aes");
        knowLedgeBaseFileMeta.setVersion("latest");
        Map<String, List<KnowLedgeBaseFileMeta>> gatewayResponse = new HashMap<>();
        gatewayResponse.put("data", List.of(knowLedgeBaseFileMeta));

        mockGatewayServer = ClientAndServer.startClientAndServer(9999);
        MockServerClient gatewayClient = new MockServerClient("localhost", 9999);

        // 定义 MockServer 的行为
        gatewayClient.when(
                HttpRequest.request()
                        .withMethod("GET")
                        .withPath("/v1/knowledge_base")
                        .withQueryStringParameter("kb_id", "1")
        ).respond(
                HttpResponse.response()
                        .withStatusCode(200)
                        .withBody(JSON.toJSONString(gatewayResponse))
        );

        mockHosServer = ClientAndServer.startClientAndServer(9098);
        MockServerClient hosClient = new MockServerClient("localhost", 9098);

        // 定义 MockServer 的行为
        hosClient.when(
                HttpRequest.request()
                        .withMethod("GET")
                        .withPath("/hos/knowledge_base_bucket/1_latest")
        ).respond(
                HttpResponse.response()
                        .withStatusCode(200)
                        .withBody(AESUtil.encrypt("test".getBytes(), "86cf0e2ffba3f541a6c6761313e5cc7e"))
        );

        AbstractSingleKnowledgeBaseHandler baseHandler = new AbstractSingleKnowledgeBaseHandler() {
            @Override
            protected Boolean buildKnowledgeBase() {
                byte[] bytes = downloadFile();
                assertEquals("test", new String(bytes));
                return true;
            }

            @Override
            public void close() {

            }
        };
        baseHandler.initKnowledgeBase(knowledgeBaseConfig);
    }

    @AfterEach
    void afterEach() {
        mockGatewayServer.stop();
        mockGatewayServer = null;
        mockHosServer.stop();
        mockHosServer = null;
    }
}