package com.geedgenetworks.core.udf.cn;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.core.pojo.KnowLedgeBaseFileMeta;
import com.geedgenetworks.core.utils.KnowlegdeBase.Handler.AbstractKnowledgeBaseHandler;
import com.geedgenetworks.core.utils.KnowlegdeBase.Handler.cn.AbstractMultipleKnowledgeBaseHandler;
import com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseUpdateJob;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mockStatic;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/26 16:45
 */
public class LookupTestUtils {

    private static String fsPath = "testPath";
    private static String fsType = "testType";
    private static List<String> fsFiles = Arrays.asList("testFile");
    public static String kbName = "testKbName";
    private static String downloadPath = "testDownloadPath";

    private static MockedStatic<AbstractKnowledgeBaseHandler> abstractKnowledgeBaseHandlerMockedStatic = mockStatic(AbstractKnowledgeBaseHandler.class);

    private static MockedStatic<AbstractMultipleKnowledgeBaseHandler> abstractMultipleKnowledgeBaseHandlerMockedStatic = mockStatic(AbstractMultipleKnowledgeBaseHandler.class);

    static RuntimeContext mockRuntimeContext() {
        RuntimeContext runtimeContext = Mockito.mock(RuntimeContext.class);
        ExecutionConfig executionConfig = Mockito.mock(ExecutionConfig.class);
        Mockito.when(runtimeContext.getExecutionConfig()).thenReturn(executionConfig);
        Configuration configuration = new Configuration();
        CommonConfig commonConfig = new CommonConfig();
        KnowledgeBaseConfig knowledgeBaseConfig = new KnowledgeBaseConfig();
        knowledgeBaseConfig.setFsPath(fsPath);
        knowledgeBaseConfig.setFsType(fsType);
        knowledgeBaseConfig.setFiles(fsFiles);
        knowledgeBaseConfig.setName(kbName);
        commonConfig.setKnowledgeBaseConfig(Arrays.asList(knowledgeBaseConfig));
        configuration.setString(Constants.SYSPROP_GROOTSTREAM_CONFIG, JSON.toJSONString(commonConfig));
        Mockito.when(executionConfig.getGlobalJobParameters()).thenReturn(configuration);
        return runtimeContext;
    }

    static void checkStaticMock() {
        if (abstractKnowledgeBaseHandlerMockedStatic.isClosed()) {
            abstractKnowledgeBaseHandlerMockedStatic = mockStatic(AbstractKnowledgeBaseHandler.class);
        }
        if (abstractMultipleKnowledgeBaseHandlerMockedStatic.isClosed()) {
            abstractMultipleKnowledgeBaseHandlerMockedStatic = mockStatic(AbstractMultipleKnowledgeBaseHandler.class);
        }
    }

    static void mockKnowledgeBaseHandler(String downloadContent) {
        checkStaticMock();
        KnowLedgeBaseFileMeta knowLedgeBaseFileMeta = new KnowLedgeBaseFileMeta();
        knowLedgeBaseFileMeta.setPath(downloadPath);
        abstractKnowledgeBaseHandlerMockedStatic.when(() -> AbstractKnowledgeBaseHandler.getMetadata(fsType, fsPath, fsFiles.get(0))).thenReturn(knowLedgeBaseFileMeta);
        abstractKnowledgeBaseHandlerMockedStatic.when(() -> AbstractKnowledgeBaseHandler.downloadFile(downloadPath, 1)).thenReturn(downloadContent.getBytes());
    }

    static void mockMultipleKnowledgeBaseHandler(String downloadContent) {
        checkStaticMock();
        KnowLedgeBaseFileMeta KnowLedgeBaseFileMeta = new KnowLedgeBaseFileMeta();
        KnowLedgeBaseFileMeta.setPath(downloadPath);
        Map<String, KnowLedgeBaseFileMeta> KnowLedgeBaseFileMetaMap = new HashMap<>();
        KnowLedgeBaseFileMetaMap.put("1", KnowLedgeBaseFileMeta);
        abstractMultipleKnowledgeBaseHandlerMockedStatic.when(() -> AbstractMultipleKnowledgeBaseHandler.getMetadata(fsPath)).thenReturn(KnowLedgeBaseFileMetaMap);
        abstractKnowledgeBaseHandlerMockedStatic.when(() -> AbstractKnowledgeBaseHandler.downloadFile(downloadPath, 1)).thenReturn(downloadContent.getBytes());
    }

    static RuntimeContext mockRuleRuntimeContext() {
        RuntimeContext runtimeContext = Mockito.mock(RuntimeContext.class);
        ExecutionConfig executionConfig = Mockito.mock(ExecutionConfig.class);
        Mockito.when(runtimeContext.getExecutionConfig()).thenReturn(executionConfig);
        Configuration configuration = new Configuration();
        CommonConfig commonConfig = new CommonConfig();
        KnowledgeBaseConfig knowledgeBaseConfig = new KnowledgeBaseConfig();
        knowledgeBaseConfig.setFsPath(fsPath);
        knowledgeBaseConfig.setFsType(fsType);
        knowledgeBaseConfig.setFiles(fsFiles);
        knowledgeBaseConfig.setName(kbName);
        KnowledgeBaseConfig ruleKnowledgeBaseConfig = new KnowledgeBaseConfig();
        ruleKnowledgeBaseConfig.setFsPath("testRulePath");
        ruleKnowledgeBaseConfig.setFsType("testRuleType");
        Map<String, String> properties = new HashMap<>();
        properties.put("token", "testToken");
        ruleKnowledgeBaseConfig.setProperties(properties);
        ruleKnowledgeBaseConfig.setName("cn_rule");
        commonConfig.setKnowledgeBaseConfig(Arrays.asList(knowledgeBaseConfig, ruleKnowledgeBaseConfig));
        configuration.setString(Constants.SYSPROP_GROOTSTREAM_CONFIG, JSON.toJSONString(commonConfig));
        Mockito.when(executionConfig.getGlobalJobParameters()).thenReturn(configuration);
        return runtimeContext;
    }

    static void clearState() {
        KnowledgeBaseUpdateJob.removeKnowledgeBase(kbName);
        abstractKnowledgeBaseHandlerMockedStatic.close();
        abstractMultipleKnowledgeBaseHandlerMockedStatic.close();
    }
}
