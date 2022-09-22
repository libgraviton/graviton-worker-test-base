package com.github.libgraviton.workertestbase;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.libgraviton.gdk.data.GravitonBase;
import com.github.libgraviton.gdk.gravitondyn.eventstatus.document.EventStatus;
import com.github.libgraviton.gdk.gravitondyn.eventstatus.document.EventStatusStatus;
import com.github.libgraviton.gdk.gravitondyn.file.document.File;
import com.github.libgraviton.gdk.gravitondyn.file.document.FileMetadata;
import com.github.libgraviton.gdk.gravitondyn.file.document.FileMetadataAction;
import com.github.libgraviton.workerbase.QueueWorkerAbstract;
import com.github.libgraviton.workerbase.exception.GravitonCommunicationException;
import com.github.libgraviton.workerbase.exception.WorkerException;
import com.github.libgraviton.workerbase.gdk.serialization.mapper.GravitonObjectMapper;
import com.github.libgraviton.workerbase.helper.DependencyInjection;
import com.github.libgraviton.workerbase.helper.WorkerProperties;
import com.github.libgraviton.workerbase.messaging.MessageAcknowledger;
import com.github.libgraviton.workerbase.model.GravitonRef;
import com.github.libgraviton.workerbase.model.QueueEvent;
import com.github.tomakehurst.wiremock.http.Body;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Before;
import org.junit.Rule;
import wiremock.org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

abstract public class BaseWorkerTest {

    @Rule
    public WireMockRule wiremock = new WireMockRule();

    protected ObjectMapper objectMapper;

    @Before
    public void prepare() throws IOException {
        objectMapper = GravitonObjectMapper.getInstance(WorkerProperties.load());

        HashMap<String, String> propertiesMap = new HashMap<>();
        propertiesMap.put("graviton.base.url", wiremock.baseUrl());
        propertiesMap.put("graviton.workerId", "testworker");
        propertiesMap.put("graviton.authentication.prefix.username", "subnet-");
        propertiesMap.put("graviton.authentication.header.name", "x-graviton-authentication");

        WorkerProperties.clearOverrides();
        WorkerProperties.addOverrides(propertiesMap);
    }

    public void prepareWorker(QueueWorkerAbstract worker) throws WorkerException, GravitonCommunicationException, IOException {
        Properties properties = WorkerProperties.load();

        DependencyInjection.init(worker, List.of());

        // add graviton base url
        stubFor(put(urlEqualTo("/event/worker/" + properties.getProperty("graviton.workerId")))
                .willReturn(aResponse().withStatus(200))
        );

        // retry interceptor does this..
        stubFor(options(urlEqualTo("/"))
                .willReturn(aResponse().withStatus(200))
        );

        worker.initialize(properties);
        worker.onStartUp();

        if (worker.shouldAutoRegister()) {
            verify(putRequestedFor(urlEqualTo("/event/worker/" + properties.getProperty("graviton.workerId"))));
        }

    }

    public void produceQueueEvent(QueueWorkerAbstract worker, GravitonBase returnObject) throws JsonProcessingException {
        QueueEvent queueEvent = getQueueEvent();

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
        documentRef.set$ref(wiremock.baseUrl() + docUrl);

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
        };

        worker.handleDelivery(queueEvent, queueEvent.getStatus().get$ref(), messageAcknowledger);


    }

    public File getFileForCommand(String command) {
        String id = getRandomString();
        File gravitonFile = new File();
        gravitonFile.setId(id);
        FileMetadata metadata = new FileMetadata();

        FileMetadataAction action = new FileMetadataAction();
        action.setCommand(command);

        metadata.setAction(List.of(action));

        gravitonFile.setMetadata(metadata);

        return gravitonFile;
    }

    public QueueEvent getQueueEvent() throws JsonProcessingException {
        String id = getRandomString();
        QueueEvent queueEvent = new QueueEvent();

        // setup eventstatus
        EventStatus eventStatus = new EventStatus();
        eventStatus.setEventName("testevent");
        eventStatus.setId(id);

        EventStatusStatus eventStatusStatus = new EventStatusStatus();
        eventStatusStatus.setStatus(EventStatusStatus.Status.OPENED);
        eventStatusStatus.setWorkerId("testworker");

        eventStatus.setStatus(List.of(eventStatusStatus));

        String eventStatusUrl = wiremock.baseUrl() + "/event/status/" + id;

        stubFor(get(urlEqualTo("/event/status/" + id))
                .willReturn(
                        aResponse().withStatus(200).withResponseBody(new Body(objectMapper.writeValueAsString(eventStatus)))
                )
        );

        // status patches
        stubFor(patch(urlEqualTo("/event/status/" + id))
                .willReturn(
                        aResponse().withStatus(200)
                )
        );

        GravitonRef ref = new GravitonRef();
        ref.set$ref(eventStatusUrl);

        queueEvent.setStatus(ref);

        return queueEvent;
    }

    public String getRandomString() {
        return RandomStringUtils.randomAlphanumeric(25);
    }
}
