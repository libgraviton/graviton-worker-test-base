package com.github.libgraviton.workertestbase;

import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.matching.MatchResult;
import com.github.tomakehurst.wiremock.matching.RequestMatcherExtension;

import java.util.ArrayList;
import java.util.List;

class WiremockKeepingTrackDoneEventStatus extends RequestMatcherExtension {

    @FunctionalInterface
    public interface OnRequestCallback {
        void onRequest(Request request);
    }

    private final List<OnRequestCallback> callbacks = new ArrayList<>();

    public WiremockKeepingTrackDoneEventStatus() {

    }

    public void registerCallback(OnRequestCallback callback) {
        callbacks.add(callback);
    }

    public void resetCallbacks() {
        callbacks.clear();
    }

    @Override
    public String getName() {
        return "keeping-track-event-status";
    }

    @Override
    public MatchResult match(Request request, Parameters parameters) {
        boolean isRelevant = (
                request.getUrl().startsWith("/event/status/")
        );

        if (!isRelevant) {
            return MatchResult.of(false);
        }

        callbacks.forEach(onRequestCallback -> onRequestCallback.onRequest(request));

        return MatchResult.of(true);
    }
}