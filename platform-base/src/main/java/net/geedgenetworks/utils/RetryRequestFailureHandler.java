package net.geedgenetworks.utils;

import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.ExceptionUtils;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Optional;

public class RetryRequestFailureHandler implements ActionRequestFailureHandler {

    @Override
    public void onFailure(ActionRequest actionRequest, Throwable throwable, int i, RequestIndexer requestIndexer) throws Throwable {
        if (ExceptionUtils.findThrowable(throwable, EsRejectedExecutionException.class).isPresent()) {
            if (actionRequest instanceof IndexRequest) {
                requestIndexer.add((IndexRequest) actionRequest);
            } else if (actionRequest instanceof DeleteRequest) {
                requestIndexer.add((DeleteRequest) actionRequest);
            } else if (actionRequest instanceof UpdateRequest) {
                requestIndexer.add((UpdateRequest) actionRequest);
            } else {
                throw new IllegalArgumentException("RequestIndexer only supports Index, Delete and Update requests");
            }
        } else {
            if (ExceptionUtils.findThrowable(throwable, SocketTimeoutException.class).isPresent()) {
                return;
            } else {
                Optional<IOException> exp = ExceptionUtils.findThrowable(throwable, IOException.class);
                if (exp.isPresent()) {
                    IOException ioExp = exp.get();
                    if (ioExp.getMessage() != null && ioExp.getMessage().contains("max retry timeout")) {
//                        log.error(ioExp.getMessage());
                        return;
                    }
                }
            }
            throw throwable;
        }
    }

}
