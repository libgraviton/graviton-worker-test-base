package com.github.libgraviton.workertestbase;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.libgraviton.gdk.data.GravitonBase;
import com.github.libgraviton.gdk.gravitondyn.file.document.File;
import com.github.libgraviton.workerbase.QueueWorkerAbstract;
import com.github.libgraviton.workerbase.exception.GravitonCommunicationException;
import com.github.libgraviton.workerbase.exception.WorkerException;
import com.github.libgraviton.workerbase.helper.DependencyInjection;
import com.github.libgraviton.workerbase.helper.WorkerProperties;
import com.github.libgraviton.workerbase.messaging.MessageAcknowledger;
import com.github.libgraviton.workerbase.model.GravitonRef;
import com.github.libgraviton.workerbase.model.QueueEvent;
import com.github.libgraviton.workertestbase.utils.TestExecutorService;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Body;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

abstract public class BaseWorkerTest {

    @RegisterExtension
    static WireMockExtension wiremock = WireMockExtension.newInstance()
            .options(WireMockConfiguration.wireMockConfig().dynamicPort().dynamicHttpsPort())
            .build();

    protected ObjectMapper objectMapper;

    @BeforeEach
    public void prepare() throws IOException {
        objectMapper = DependencyInjection.getInstance(ObjectMapper.class);

        HashMap<String, String> propertiesMap = new HashMap<>();
        propertiesMap.put(WorkerProperties.GRAVITON_BASE_URL, wiremock.baseUrl());
        propertiesMap.put(WorkerProperties.WORKER_ID, "testworker");
        propertiesMap.put(WorkerProperties.AUTH_PREFIX_USERNAME, "subnet-");
        propertiesMap.put(WorkerProperties.AUTH_HEADER_NAME, "x-graviton-authentication");

        WorkerProperties.clearOverrides();
        WorkerProperties.addOverrides(propertiesMap);
    }

    @AfterEach
    public void reset() {
        WorkerProperties.clearOverrides();
        DependencyInjection.clearInstanceOverrides();
    }

    public void prepareWorker(QueueWorkerAbstract worker) throws WorkerException, GravitonCommunicationException, IOException {
        Properties properties = WorkerProperties.load();

        DependencyInjection.init(List.of());

        // dummy executorService for async cases
        DependencyInjection.addInstanceOverride(ExecutorService.class, new TestExecutorService());

        // add graviton base url
        stubFor(put(urlEqualTo("/event/worker/" + properties.getProperty("graviton.workerId")))
                .willReturn(aResponse().withStatus(200))
        );

        // retry interceptor does this..
        stubFor(options(urlEqualTo("/"))
                .willReturn(aResponse().withStatus(200))
        );

        worker.onStartUp();

        if (worker.shouldAutoRegister()) {
            verify(putRequestedFor(urlEqualTo("/event/worker/" + properties.getProperty("graviton.workerId"))));
        }
    }

    public QueueEvent produceQueueEvent(QueueWorkerAbstract worker, GravitonBase returnObject) throws JsonProcessingException {
        QueueEvent queueEvent = TestUtils.getQueueEvent();

        stubFor(get(urlEqualTo("/event/status/" + queueEvent.getEvent()))
                .willReturn(
                        aResponse().withStatus(200).withResponseBody(new Body(objectMapper.writeValueAsString(queueEvent)))
                )
        );

        // status patches
        stubFor(patch(urlEqualTo("/event/status/" + queueEvent.getEvent()))
                .willReturn(
                        aResponse().withStatus(200)
                )
        );


        // special case file..
        boolean isFile = (returnObject instanceof File);

        // wire returnObject to document
        String docUrl;
        if (!isFile) {
            docUrl = "/documents/" + returnObject.getId();
        } else {
            docUrl = "/file/" + returnObject.getId();
        }

        GravitonRef documentRef = new GravitonRef();
        documentRef.set$ref(WorkerProperties.getProperty(WorkerProperties.GRAVITON_BASE_URL) + docUrl);

        queueEvent.setDocument(documentRef);

        stubFor(get(urlEqualTo(docUrl)).withHeader("Accept", containing("application/json"))
                .willReturn(
                        aResponse().withStatus(200).withResponseBody(new Body(objectMapper.writeValueAsString(returnObject)))
                )
        );

        // patch to this should be accepted
        stubFor(patch(urlMatching(".*\\/" + returnObject.getId()))
                .willReturn(
                        aResponse().withStatus(200)
                )
        );

        MessageAcknowledger messageAcknowledger = new MessageAcknowledger() {
            private boolean called = false;
            @Override
            public void acknowledge(String messageId) {
                called = true;
            }
            @Override
            public void acknowledgeFail(String messageId) {
                called = true;
            }
        };


        //worker.handleDelivery(queueEvent, queueEvent.getStatus().get$ref(), messageAcknowledger);

        return queueEvent;
    }
}
