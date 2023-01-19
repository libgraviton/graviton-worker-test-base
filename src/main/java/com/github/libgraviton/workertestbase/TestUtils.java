package com.github.libgraviton.workertestbase;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.libgraviton.gdk.gravitondyn.eventstatus.document.EventStatus;
import com.github.libgraviton.gdk.gravitondyn.eventstatus.document.EventStatusStatus;
import com.github.libgraviton.workerbase.helper.DependencyInjection;
import com.github.libgraviton.workerbase.helper.WorkerProperties;
import com.github.libgraviton.workerbase.model.GravitonRef;
import com.github.libgraviton.workerbase.model.QueueEvent;
import com.github.tomakehurst.wiremock.http.Body;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;

public class TestUtils {

    public static String getRandomString() {
        return getRandomString(25);
    }

    public static String getRandomString(int length) {
        return RandomStringUtils.randomAlphanumeric(length);
    }


    public static QueueEvent getQueueEvent() throws JsonProcessingException {

        ObjectMapper objectMapper = DependencyInjection.getInstance(ObjectMapper.class);

        String id = getRandomString();
        QueueEvent queueEvent = new QueueEvent();

        // setup eventstatus
        EventStatus eventStatus = new EventStatus();
        eventStatus.setEventName("testevent");
        eventStatus.setId(id);

        EventStatusStatus eventStatusStatus = new EventStatusStatus();
        eventStatusStatus.setStatus(EventStatusStatus.Status.OPENED);
        //eventStatusStatus.setId(id);
        eventStatusStatus.setWorkerId("testworker");

        eventStatus.setStatus(List.of(eventStatusStatus));

        String eventStatusUrl = WorkerProperties.GRAVITON_BASE_URL.get() + "/event/status/" + id;

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

}
