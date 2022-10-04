package com.github.libgraviton.workertestbase;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class DummyGraviton {
    public static void main(String[] args) {
        WireMockServer wireMockServer = new WireMockServer(WireMockConfiguration.options().port(8080)); //No-args constructor will start on port 8080, no HTTPS
        wireMockServer.start();

        wireMockServer.stubFor(post(urlEqualTo("/event/worker"))
                .willReturn(
                        aResponse().withStatus(201)
                )
        );

        wireMockServer.stubFor(put(urlMatching("/event/worker/(.*)"))
                .willReturn(
                        aResponse().withStatus(201)
                )
        );
    }
}
