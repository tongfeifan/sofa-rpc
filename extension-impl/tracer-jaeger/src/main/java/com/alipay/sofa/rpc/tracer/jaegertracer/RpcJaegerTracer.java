/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.rpc.tracer.jaegertracer;


import com.alipay.sofa.rpc.common.RpcConstants;
import com.alipay.sofa.rpc.common.utils.NetUtils;
import com.alipay.sofa.rpc.common.utils.StringUtils;
import com.alipay.sofa.rpc.context.RpcInternalContext;
import com.alipay.sofa.rpc.core.exception.RpcErrorType;
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import com.alipay.sofa.rpc.core.request.SofaRequest;
import com.alipay.sofa.rpc.core.response.SofaResponse;
import com.alipay.sofa.rpc.ext.Extension;
import com.alipay.sofa.rpc.tracer.Tracer;
import com.alipay.sofa.rpc.tracer.jaegertracer.code.TracerResultCode;
import com.alipay.sofa.rpc.tracer.jaegertracer.tags.RpcSpanTags;
import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.JaegerTracer;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * SofaTracer
 *
 */
@Extension("jaegerTracer")
public class RpcJaegerTracer extends Tracer {

    /***
     * tracer 类型
     */
    public static final String RPC_TRACER_TYPE = "RPC_TRACER";

    /***
     * 发生错误时用于标示错误源信息
     */
    public static final String ERROR_SOURCE    = "rpc";

    public static final String CARRIER = "carrier";

    private JaegerTracer jaegerTracer;

    public RpcJaegerTracer() {

        //构造 RPC 的 tracer 实例
        jaegerTracer = (JaegerTracer) GlobalTracer.get();

    }

    @Override
    public void startRpc(SofaRequest request) {
        //客户端的启动

        Span serverSpan = this.jaegerTracer.activeSpan();

        JaegerSpan clientSpan = (JaegerSpan) this.jaegerTracer.buildSpan(request.getInterfaceName())
            .asChildOf(serverSpan)
            .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
            .startActive(true).span();

        if (RpcInternalContext.isAttachmentEnable()) {
            RpcInternalContext context = RpcInternalContext.getContext();
            clientSpan
                .setTag(RpcSpanTags.LOCAL_APP, (String) context.getAttachment(RpcConstants.INTERNAL_KEY_APP_NAME));
            clientSpan.setTag(RpcSpanTags.PROTOCOL,
                (String) context.getAttachment(RpcConstants.INTERNAL_KEY_PROTOCOL_NAME));
            JaegerSpanContext spanContext = clientSpan.context();
            if (spanContext != null) {
                context.setAttachment(RpcConstants.INTERNAL_KEY_TRACE_ID, spanContext.getTraceId());
                context.setAttachment(RpcConstants.INTERNAL_KEY_SPAN_ID, spanContext.getSpanId());
            }
        }

        clientSpan.setTag(RpcSpanTags.SERVICE, request.getTargetServiceUniqueName());
        clientSpan.setTag(RpcSpanTags.METHOD, request.getMethodName());
        clientSpan.setTag(RpcSpanTags.CURRENT_THREAD_NAME, Thread.currentThread().getName());

    }

    public class RequestBuilderCarrier implements io.opentracing.propagation.TextMap {
        private final SofaRequest builder;

        RequestBuilderCarrier(SofaRequest builder) {
            this.builder = builder;
            builder.addRequestProp("carrier", new HashMap<String, String>());
        }

        @Override
        public Iterator<Map.Entry<String, String>> iterator() {
            throw new UnsupportedOperationException("carrier is write-only");
        }

        @Override
        public void put(String key, String value) {
            Map<String, String> carrierMap = (Map<String, String>) builder.getRequestProp(CARRIER);
            carrierMap.put(key, value);
        }
    }

    @Override
    public void clientBeforeSend(SofaRequest request) {
        //获取 RPC 上下文
        RpcInternalContext rpcInternalContext = RpcInternalContext.getContext();
        Tags.SPAN_KIND.set(jaegerTracer.activeSpan(), Tags.SPAN_KIND_CLIENT);
        Tags.HTTP_METHOD.set(jaegerTracer.activeSpan(), request.getMethodName());
        jaegerTracer.inject(jaegerTracer.activeSpan().context(), Format.Builtin.TEXT_MAP, new RequestBuilderCarrier(request));



        // 异步callback同步
        if (request.isAsync()) {
            //异步,这个时候除了缓存spanContext clientBeforeSendRequest() rpc 已经调用
            //还需要这个时候需要还原回父 span
            //弹出;不弹出的话当前线程就会一直是client了
            JaegerSpan clientSpan = (JaegerSpan) jaegerTracer.activeSpan();
            rpcInternalContext.setAttachment(RpcConstants.INTERNAL_KEY_TRACER_SPAN, clientSpan);

            if (clientSpan != null) {
                // Record client send event
                clientSpan.log("client send");
                jaegerTracer.scopeManager().active().close();

            }
        } else {
            // Record client send event
            jaegerTracer.activeSpan().log("client send");
        }

    }


    @Override
    public void clientReceived(SofaRequest request, SofaResponse response, Throwable exceptionThrow) {
        JaegerSpan clientSpan = (JaegerSpan) jaegerTracer.activeSpan();
        //客户端的启动
        if (clientSpan == null) {
            return;
        }
        // Record client receive event
        clientSpan.log("client receive");
        //rpc 上下文
        RpcInternalContext context = null;
        if (RpcInternalContext.isAttachmentEnable()) {
            context = RpcInternalContext.getContext();

            if (!clientSpan.getTags().containsKey(RpcSpanTags.ROUTE_RECORD)) {
                clientSpan.setTag(RpcSpanTags.ROUTE_RECORD,
                    (String) context.getAttachment(RpcConstants.INTERNAL_KEY_ROUTER_RECORD));
            }
            clientSpan.setTag(RpcSpanTags.REQ_SERIALIZE_TIME,
                (Number) context.getAttachment(RpcConstants.INTERNAL_KEY_REQ_SERIALIZE_TIME));
            clientSpan.setTag(RpcSpanTags.RESP_DESERIALIZE_TIME,
                (Number) context.getAttachment(RpcConstants.INTERNAL_KEY_RESP_DESERIALIZE_TIME));
            clientSpan.setTag(RpcSpanTags.RESP_SIZE,
                (Number) context.getAttachment(RpcConstants.INTERNAL_KEY_RESP_SIZE));
            clientSpan.setTag(RpcSpanTags.REQ_SIZE, (Number) context.getAttachment(RpcConstants.INTERNAL_KEY_REQ_SIZE));
            clientSpan.setTag(RpcSpanTags.CLIENT_CONN_TIME,
                (Number) context.getAttachment(RpcConstants.INTERNAL_KEY_CONN_CREATE_TIME));

            Long ce = (Long) context.getAttachment(RpcConstants.INTERNAL_KEY_CLIENT_ELAPSE);
            if (ce != null) {
                clientSpan.setTag(RpcSpanTags.CLIENT_ELAPSE_TIME, ce);
            }

            InetSocketAddress address = context.getLocalAddress();
            if (address != null) {
                clientSpan.setTag(RpcSpanTags.LOCAL_IP, NetUtils.toIpString(address));
                clientSpan.setTag(RpcSpanTags.LOCAL_PORT, address.getPort());
            }
        }

        Throwable throwableShow = exceptionThrow;
        // 区分出各个异常信息
        String resultCode = StringUtils.EMPTY;

        if (throwableShow != null) {
            // 客户端异常
            if (throwableShow instanceof SofaRpcException) {
                SofaRpcException exception = (SofaRpcException) throwableShow;
                //摘要打印
                int errorType = exception.getErrorType();
                switch (errorType) {
                    case RpcErrorType.CLIENT_TIMEOUT:
                        resultCode = TracerResultCode.RPC_RESULT_TIMEOUT_FAILED;
                        break;
                    case RpcErrorType.CLIENT_ROUTER:
                        resultCode = TracerResultCode.RPC_RESULT_ROUTE_FAILED;
                        break;
                    case RpcErrorType.CLIENT_SERIALIZE:
                    case RpcErrorType.CLIENT_DESERIALIZE:
                        resultCode = TracerResultCode.RPC_RESULT_RPC_FAILED;
                        break;
                    default:
                        resultCode = TracerResultCode.RPC_RESULT_RPC_FAILED;
                        break;
                }
            } else {
                // 这里是客户端的未知异常，目前不会走到这里
                resultCode = TracerResultCode.RPC_RESULT_RPC_FAILED;
            }

        } else if (response != null) {
            // 服务端rpc异常
            if (response.isError()) {
                resultCode = TracerResultCode.RPC_RESULT_RPC_FAILED;
                //客户端服务端均打印
                throwableShow = new SofaRpcException(RpcErrorType.SERVER_UNDECLARED_ERROR, response.getErrorMsg());
            } else {
                Object ret = response.getAppResponse();
                if (ret instanceof Throwable) {
                    throwableShow = (Throwable) ret;
                    // 业务异常
                    resultCode = TracerResultCode.RPC_RESULT_BIZ_FAILED;
                } else {
                    resultCode = TracerResultCode.RPC_RESULT_SUCCESS;
                }
            }
        }
        if (throwableShow != null) {
            Map<String, Object> exceptionLogs = new HashMap<String, Object>();
            exceptionLogs.put("event", Tags.ERROR.getKey());
            exceptionLogs.put("error.object", throwableShow);
            //记录的上下文信息
            this.generateClientErrorContext(exceptionLogs, request, clientSpan);
            clientSpan.log(exceptionLogs);
            Tags.ERROR.set(clientSpan, true);
        }
        clientSpan.setTag(RpcSpanTags.RESULT_CODE, resultCode);
        if (context != null) {
            context.setAttachment(RpcConstants.INTERNAL_KEY_RESULT_CODE, resultCode);
        }
        //finish client
        jaegerTracer.scopeManager().active().close();
    }

    private void generateClientErrorContext(Map<String, Object> context, SofaRequest request, JaegerSpan clientSpan) {
        Map<String, Object> tags = clientSpan.getTags();
        //记录的上下文信息
        context.put("serviceName", tags.get(RpcSpanTags.SERVICE));
        context.put("methodName", tags.get(RpcSpanTags.METHOD));
        context.put("protocol", tags.get(RpcSpanTags.PROTOCOL));
        context.put("invokeType", tags.get(RpcSpanTags.INVOKE_TYPE));
        context.put("targetUrl", tags.get(RpcSpanTags.REMOTE_IP));
        context.put("targetApp", tags.get(RpcSpanTags.REMOTE_APP));
        context.put("targetZone", tags.get(RpcSpanTags.REMOTE_ZONE));
        context.put("targetIdc", tags.get(RpcSpanTags.REMOTE_IDC));
        context.put("paramTypes", request.getMethodArgSigs());
        context.put("targetCity", tags.get(RpcSpanTags.REMOTE_CITY));
        context.put("uid", tags.get(RpcSpanTags.USER_ID));
    }

    @Override
    public void serverReceived(SofaRequest request) {

        Map<String, String> tags = new HashMap<String, String>();
        //server tags 必须设置
        tags.put(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER);

        SpanContext parentSpanContext = jaegerTracer.extract(Format.Builtin.HTTP_HEADERS, new TextMapExtractAdapter((Map<String, String>) request.getRequestProp(CARRIER)));

        JaegerSpan serverSpan = (JaegerSpan) jaegerTracer.buildSpan(request.getInterfaceName())
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER)
                .asChildOf(parentSpanContext).startActive(true);
        // Record server receive event
        serverSpan.log("server receive");
        //rpc 上下文
        if (RpcInternalContext.isAttachmentEnable()) {
            RpcInternalContext context = RpcInternalContext.getContext();
            context.setAttachment(RpcConstants.INTERNAL_KEY_TRACE_ID, ((JaegerSpanContext) parentSpanContext).getTraceId());
            context.setAttachment(RpcConstants.INTERNAL_KEY_SPAN_ID, ((JaegerSpanContext) parentSpanContext).getSpanId());
        }
    }


    @Override
    public void serverSend(SofaRequest request, SofaResponse response, Throwable exception) {
        JaegerSpan serverSpan = (JaegerSpan) jaegerTracer.activeSpan();
        if (serverSpan == null) {
            return;
        }
        // Record server send event
        serverSpan.log("server send");

        RpcInternalContext context = RpcInternalContext.getContext();
        serverSpan.setTag(RpcSpanTags.RESP_SERIALIZE_TIME,
            (Number) context.getAttachment(RpcConstants.INTERNAL_KEY_RESP_SERIALIZE_TIME));
        serverSpan.setTag(RpcSpanTags.REQ_DESERIALIZE_TIME,
            (Number) context.getAttachment(RpcConstants.INTERNAL_KEY_REQ_DESERIALIZE_TIME));
        serverSpan.setTag(RpcSpanTags.RESP_SIZE, (Number) context.getAttachment(RpcConstants.INTERNAL_KEY_RESP_SIZE));
        serverSpan.setTag(RpcSpanTags.REQ_SIZE, (Number) context.getAttachment(RpcConstants.INTERNAL_KEY_REQ_SIZE));
        //当前线程名
        serverSpan.setTag(RpcSpanTags.CURRENT_THREAD_NAME, Thread.currentThread().getName());

        Throwable throwableShow = exception;
        String resultCode = StringUtils.EMPTY;
        if (throwableShow != null) {
            //当前即服务端应用
            // 不会业务异常
            resultCode = TracerResultCode.RPC_RESULT_RPC_FAILED;
        } else if (response != null) {
            // 判断是否是业务异常
            if (response.isError()) {
                resultCode = TracerResultCode.RPC_RESULT_RPC_FAILED;
                //改变打印的 throwable
                throwableShow = new SofaRpcException(RpcErrorType.SERVER_UNDECLARED_ERROR, response.getErrorMsg());
            } else {
                Object ret = response.getAppResponse();
                if (ret instanceof Throwable) {
                    throwableShow = (Throwable) ret;
                    // 业务异常
                    resultCode = TracerResultCode.RPC_RESULT_BIZ_FAILED;
                } else {
                    resultCode = TracerResultCode.RPC_RESULT_SUCCESS;
                }
            }
        }
        if (throwableShow != null) {
            // 打印错误
            // result code
            Map<String, Object> exceptionLogs = new HashMap<String, Object>();
            exceptionLogs.put("event", Tags.ERROR.getKey());
            exceptionLogs.put("error.object", throwableShow);
            //记录的上下文信息
            this.generateServerErrorContext(exceptionLogs, request, serverSpan);
            serverSpan.log(exceptionLogs);
            Tags.ERROR.set(serverSpan, true);
        }
        // 结果码（00=成功/01=业务异常/02=RPC逻辑错误）
        serverSpan.setTag(RpcSpanTags.RESULT_CODE, resultCode);
        serverSpan.finish();
    }

    private void generateServerErrorContext(Map<String, Object> context, SofaRequest request,
                                            JaegerSpan serverSpan) {
        //tags
        context.put("serviceName", serverSpan.getTags().get(RpcSpanTags.SERVICE));
        context.put("methodName",  serverSpan.getTags().get(RpcSpanTags.METHOD));
        context.put("protocol", serverSpan.getTags().get(RpcSpanTags.PROTOCOL));
        context.put("invokeType", serverSpan.getTags().get(RpcSpanTags.INVOKE_TYPE));

        context.put("callerUrl", serverSpan.getTags().get(RpcSpanTags.REMOTE_IP));
        context.put("callerApp",  serverSpan.getTags().get(RpcSpanTags.REMOTE_APP));
        context.put("callerZone",  serverSpan.getTags().get(RpcSpanTags.REMOTE_ZONE));
        context.put("callerIdc",  serverSpan.getTags().get(RpcSpanTags.REMOTE_IDC));
        //paramTypes
        if (request != null) {
            context.put("paramTypes", request.getMethodArgSigs().toString());
        }
    }

    @Override
    public void clientAsyncAfterSend(SofaRequest request) {
        //do nothing
    }

    @Override
    public void clientAsyncReceivedPrepare() {
        //新的线程
        RpcInternalContext rpcInternalContext = RpcInternalContext.getContext();
        JaegerSpan clientSpan = (JaegerSpan)
                rpcInternalContext.getAttachment(RpcConstants.INTERNAL_KEY_TRACER_SPAN);
        if (clientSpan == null) {
            return;
        }
        jaegerTracer.scopeManager().activate(clientSpan, true);
    }

    @Override
    public void checkState() {
        // do noting
    }

    @Override
    public void profile(String profileApp, String code, String message) {
        // do nothing
    }

    public JaegerTracer getSofaTracer() {
        return jaegerTracer;
    }
}
