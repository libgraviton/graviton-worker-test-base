package com.github.libgraviton.workertestbase;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.libgraviton.gdk.data.GravitonBase;
import com.github.libgraviton.gdk.gravitondyn.eventstatus.document.EventStatus;
import com.github.libgraviton.gdk.gravitondyn.eventstatus.document.EventStatusStatus;
import com.github.libgraviton.gdk.gravitondyn.file.document.File;
import com.github.libgraviton.workerbase.QueueManager;
import com.github.libgraviton.workerbase.WorkerInterface;
import com.github.libgraviton.workerbase.WorkerLauncher;
import com.github.libgraviton.workerbase.helper.DependencyInjection;
import com.github.libgraviton.workerbase.helper.WorkerProperties;
import com.github.libgraviton.workerbase.messaging.exception.CannotPublishMessage;
import com.github.libgraviton.workerbase.model.GravitonRef;
import com.github.libgraviton.workerbase.model.QueueEvent;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Body;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertManyResult;
import okhttp3.HttpUrl;
import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.json.JSONObject;
import org.junit.jupiter.api.extension.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class WorkerTestExtension implements
        BeforeEachCallback,
        AfterEachCallback,
        BeforeAllCallback,
        AfterAllCallback {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerTestExtension.class);
    private boolean startWiremock = false;

    private final String logPrefix = "**** TEST EXTENSION ****";

    /**
     * mongodb
     */
    private boolean startMongodb = false;
    private String mongodbImage = "mongo:6.0";
    private final HashMap<String, String> mongoDbResourcesClassPathMapping = new HashMap<>();
    private String mongoDbName = "db";

    /**
     * rabbitmq
     */
    private boolean startRabbitMq = false;
    private String rabbitmqImage = "rabbitmq:3-management";
    private final HashMap<String, String> rabbitmqResourcesClassPathMapping = new HashMap<>();

    protected static ObjectMapper objectMapper;

    protected static WireMockServer wireMockServer;
    protected WiremockKeepingTrackDoneEventStatus wireMockKeepingTrackDoneEventStatus = new WiremockKeepingTrackDoneEventStatus();

    protected static RabbitMQContainer rabbitMQContainer;
    protected static ConfigureRabbitMQContainer configureRabbitMQContainer;

    protected static MongoDBContainer mongoDBContainer;
    protected static ConfigureMongoDbContainer configureMongoDBContainer;
    protected static QueueManager queueManager;

    @FunctionalInterface
    public interface ConfigureMongoDbContainer {
        MongoDBContainer configure(MongoDBContainer mongoDBContainer);
    }
    @FunctionalInterface
    public interface ConfigureRabbitMQContainer {
        RabbitMQContainer configure(RabbitMQContainer rabbitMQContainer);
    }

    public WorkerTestExtension configureRabbitMQContainer(ConfigureRabbitMQContainer configureRabbitMQContainer2) {
        configureRabbitMQContainer = configureRabbitMQContainer2;
        return this;
    }

    public WorkerTestExtension configureMongoDBContainer(ConfigureMongoDbContainer configureMongoDBContainer2) {
        configureMongoDBContainer = configureMongoDBContainer2;
        return this;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        WorkerProperties.load();
        LOG.info("{} Loaded all properties", logPrefix);

        if (startWiremock) {
            startWiremock();
            WorkerProperties.setOverride(WorkerProperties.GRAVITON_BASE_URL.toString(), getWiremockUrl());
            LOG.info(
                    "{} Started Wiremock at {} (http) and {} (https)",
                    logPrefix,
                    getWiremockUrl(),
                    getWiremockUrl(true)
            );
        }

        DependencyInjection.init();

        LOG.info("{} Dependency injection initialized.", logPrefix);

        objectMapper = DependencyInjection.getInstance(ObjectMapper.class);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        if (wireMockServer != null) {
            wireMockServer.stop();
            wireMockServer = null;
            LOG.info("{} Stopped Wiremock.", logPrefix);
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        if (startRabbitMq) {
            startRabbitMq();
            queueManager = new QueueManager(WorkerProperties.load());
            DependencyInjection.addInstanceOverride(QueueManager.class, queueManager);
        }
        if (startMongodb) {
            startMongoDb();
        }
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        if (queueManager != null) {
            queueManager.close();
            queueManager = null;
            LOG.info("{} Reset QueueManager.", logPrefix);
        }
        if (rabbitMQContainer != null) {
            rabbitMQContainer.stop();
            rabbitMQContainer = null;
            LOG.info("{} Stopped RabbitMQ.", logPrefix);
        }
        if (mongoDBContainer != null) {
            mongoDBContainer.stop();
            mongoDBContainer = null;
            LOG.info("{} Stopped MongoDB.", logPrefix);
        }

        WorkerProperties.clearOverrides();

        if (wireMockServer != null) {
            resetWiremock();
            LOG.info("{} Wiremock: Reset finished.", logPrefix);
        }

        wireMockKeepingTrackDoneEventStatus.resetCallbacks();
    }

    public String getResourceFileContent(String name) throws IOException {
        String content = null;
        try (InputStream contentStream = getClass().getClassLoader().getResourceAsStream(name)) {
            if (contentStream != null) {
                content = IOUtils.toString(contentStream, StandardCharsets.UTF_8);
            }
        }

        return content;
    }

    public WorkerTestExtension setStartWiremock(boolean startWiremock) {
        this.startWiremock = startWiremock;
        return this;
    }

    public WorkerTestExtension setMongoDbImage(String mongodbImage) {
        this.mongodbImage = mongodbImage;
        return this;
    }

    public WorkerTestExtension addMongoDbClassmapResourceMapping(String resourceName, String mappingPath) {
        mongoDbResourcesClassPathMapping.put(resourceName, mappingPath);
        return this;
    }

    public WorkerTestExtension setMongoDbName(String mongoDbName) {
        this.mongoDbName = mongoDbName;
        return this;
    }

    public MongoClient getMongoClient() {
        return MongoClients.create(mongoDBContainer.getConnectionString());
    }

    public MongoCollection<Document> getMongoCollection(String name) {
        return getMongoClient().getDatabase(mongoDbName).getCollection(name);
    }

    public InsertManyResult loadMongoDbFixtures(String collectionName, Document... doc) {
        MongoCollection<Document> coll = getMongoClient().getDatabase(mongoDbName).getCollection(collectionName);
        return coll.insertMany(List.of(doc));
    }

    public WorkerTestExtension setStartRabbitMq(boolean startRabbitMq) {
        this.startRabbitMq = startRabbitMq;
        return this;
    }

    public WorkerTestExtension setStartMongoDb(boolean startMongodb) {
        this.startMongodb = startMongodb;
        return this;
    }

    public WorkerTestExtension setRabbitmqImage(String rabbitmqImage) {
        this.rabbitmqImage = rabbitmqImage;
        return this;
    }

    public WorkerTestExtension addRabbitMqClassmapResourceMapping(String resourceName, String mappingPath) {
        rabbitmqResourcesClassPathMapping.put(resourceName, mappingPath);
        return this;
    }

    public RabbitMQContainer getRabbitMQContainer() {
        return rabbitMQContainer;
    }

    public MongoDBContainer getMongoDBContainer() {
        return mongoDBContainer;
    }

    public WireMockServer getWireMockServer() {
        return wireMockServer;
    }

    public QueueEvent getQueueEvent() throws JsonProcessingException {
        return getQueueEvent(Map.of(), null, null);
    }

    public QueueEvent getQueueEvent(Map<String, String> transientHeaders) throws JsonProcessingException {
        return getQueueEvent(transientHeaders, null, null);
    }

    public QueueEvent getQueueEvent(Map<String, String> transientHeaders, String coreUserId) throws JsonProcessingException {
        return getQueueEvent(transientHeaders, coreUserId, null);
    }

    public QueueEvent getQueueEvent(Map<String, String> transientHeaders, String coreUserId, GravitonBase returnObject) throws JsonProcessingException {
        String id = TestUtils.getRandomString();

        QueueEvent queueEvent = new QueueEvent();
        queueEvent.setCoreUserId(coreUserId);
        queueEvent.setTransientHeaders(transientHeaders);
        queueEvent.setEvent(id);

        // ref!
        if (returnObject != null) {
            boolean isFile = (returnObject instanceof File);

            // wire returnObject to document
            String docUrl;
            if (!isFile) {
                docUrl = "/documents/" + returnObject.getId();
            } else {
                docUrl = "/file/" + returnObject.getId();
            }

            GravitonRef documentRef = new GravitonRef();
            documentRef.set$ref(WorkerProperties.GRAVITON_BASE_URL.get() + docUrl);

            queueEvent.setDocument(documentRef);

            MappingBuilder stub = get(urlEqualTo(docUrl));
            transientHeaders.forEach((k, v) -> stub.withHeader(k, equalTo(v)));

            wireMockServer.stubFor(stub
                    .withHeader("Accept", containing("application/json"))
                    .withHeader(WorkerProperties.AUTH_HEADER_NAME.get(), equalTo(WorkerProperties.AUTH_PREFIX_USERNAME.get()
                            .concat(WorkerProperties.WORKER_ID.get())))
                    .willReturn(
                            aResponse().withStatus(200).withResponseBody(new Body(objectMapper.writeValueAsString(returnObject)))
                    )
            );
            wireMockServer.stubFor(patch(urlEqualTo(docUrl))
                    .withHeader(WorkerProperties.AUTH_HEADER_NAME.get(), equalTo(WorkerProperties.AUTH_PREFIX_USERNAME.get()
                            .concat(WorkerProperties.WORKER_ID.get())))
                            .andMatching(wireMockKeepingTrackDoneEventStatus)
                    .willReturn(
                            aResponse().withStatus(201)
                    )
            );
        }

        // setup eventstatus
        EventStatus eventStatus = new EventStatus();
        eventStatus.setEventName("testevent");
        eventStatus.setId(id);

        EventStatusStatus eventStatusStatus = new EventStatusStatus();
        eventStatusStatus.setStatus(EventStatusStatus.Status.OPENED);
        eventStatusStatus.setWorkerId(WorkerProperties.WORKER_ID.get());

        eventStatus.setStatus(List.of(eventStatusStatus));

        String eventStatusUrl = WorkerProperties.GRAVITON_BASE_URL.get() + "/event/status/" + id;

        LOG.info("{} EVENT STATUS URL {}", logPrefix, eventStatusUrl);

        wireMockServer.stubFor(get(urlEqualTo("/event/status/" + id))
                .withHeader(WorkerProperties.AUTH_HEADER_NAME.get(), equalTo(WorkerProperties.AUTH_PREFIX_USERNAME.get()
                        .concat(WorkerProperties.WORKER_ID.get())))
                .willReturn(
                        aResponse().withStatus(200).withResponseBody(new Body(objectMapper.writeValueAsString(eventStatus)))
                )
                .atPriority(100)
        );

        // status patches
        wireMockServer.stubFor(patch(urlEqualTo("/event/status/" + id))
                .withHeader(WorkerProperties.AUTH_HEADER_NAME.get(), equalTo(WorkerProperties.AUTH_PREFIX_USERNAME.get()
                        .concat(WorkerProperties.WORKER_ID.get())))
                .willReturn(
                        aResponse().withStatus(200)
                )

                .atPriority(100)
        );

        GravitonRef ref = new GravitonRef();
        ref.set$ref(eventStatusUrl);

        queueEvent.setStatus(ref);

        return queueEvent;
    }

    public CountDownLatch getCountDownLatchForSelfTrackingWorkers(int number) {
        final CountDownLatch latch = new CountDownLatch(number);

        wireMockKeepingTrackDoneEventStatus.registerCallback(request -> {
            boolean doneState = request.getBodyAsString().contains("\"done\"") || request.getBodyAsString().contains("\"failed\"");

            if (doneState) {
                latch.countDown();
            }
        });

        return latch;
    }

    public CountDownLatch getCountDownLatch(int countdown, WorkerLauncher launcher) {
        final CountDownLatch countDownLatch = new CountDownLatch(countdown);
        launcher.getQueueWorkerRunner().addOnCompleteCallback((duration) -> {
            countDownLatch.countDown();
        });
        return countDownLatch;
    }

    public void verifyQueueEventWasDone(QueueEvent queueEvent) {
        verifyQueueEventWasSetToStatus(queueEvent, "working");
        verifyQueueEventWasSetToStatus(queueEvent, "done");
    }

    public void verifyQueueEventWasSetToStatus(QueueEvent queueEvent, String status) {
        getWireMockServer().verify(
                1,
                patchRequestedFor(urlEqualTo("/event/status/" + queueEvent.getEvent()))
                        .withRequestBody(containing("\""+status+"\""))
        );
    }

    public void verifyQueueEventWasSetToFailed(QueueEvent queueEvent, String errorMessage) {
        verifyQueueEventWasSetToStatus(queueEvent, "working");
        verifyQueueEventWasSetToStatus(queueEvent, "failed");
        getWireMockServer().verify(
                1,
                patchRequestedFor(urlEqualTo("/event/status/" + queueEvent.getEvent()))
                        .withRequestBody(containing(errorMessage))
        );
    }

    public String prepareGatewayLogin(String username, String password) {

        String token = TestUtils.getRandomString(60);
        JSONObject authResponse = new JSONObject();
        authResponse.put("token", token);

        wireMockServer.stubFor(post(urlEqualTo("/auth"))
                        .withRequestBody(and(
                                containing("username"),
                                containing("password"),
                                containing(username),
                                containing(password)
                        ))
                .willReturn(
                        aResponse().withStatus(200).withResponseBody(new Body(authResponse.toString()))
                )
                .atPriority(100)
        );

        wireMockServer.stubFor(get(urlEqualTo("/security/logout"))
                .withHeader("x-rest-token", equalTo(token))
                .willReturn(
                        aResponse().withStatus(200)
                )
        );

        return token;
    }

    public WorkerLauncher getWrappedWorker(Class<? extends WorkerInterface> clazz) throws Exception {
        Properties properties = DependencyInjection.getInstance(Properties.class);
        WorkerInterface worker = DependencyInjection.getInstance(clazz);

        return new WorkerLauncher(
                worker,
                properties
        );
    }


    public void sendToWorker(QueueEvent queueEvent) throws JsonProcessingException, CannotPublishMessage {
        queueManager.publish(objectMapper.writeValueAsString(queueEvent));
        LOG.info("{} Sent to queue: {}", logPrefix, objectMapper.writeValueAsString(queueEvent));
    }

    private void resetWiremock() {
        WorkerProperties.setOverride(WorkerProperties.GRAVITON_BASE_URL.toString(), getWiremockUrl());

        wireMockServer.resetAll();
        wireMockServer.stubFor(options(urlEqualTo("/"))
                .willReturn(
                        aResponse().withStatus(200)
                )
                .atPriority(Integer.MAX_VALUE)
        );

        wireMockServer.stubFor(post(urlEqualTo("/event/worker"))
                .withHeader(WorkerProperties.AUTH_HEADER_NAME.get(), equalTo(WorkerProperties.AUTH_PREFIX_USERNAME.get()
                        .concat(WorkerProperties.WORKER_ID.get())))
                .willReturn(
                        aResponse().withStatus(201)
                )
                .atPriority(Integer.MAX_VALUE)
        );

        wireMockServer.stubFor(put(urlMatching("/event/worker/(.*)"))
                .withHeader(WorkerProperties.AUTH_HEADER_NAME.get(), equalTo(WorkerProperties.AUTH_PREFIX_USERNAME.get()
                        .concat(WorkerProperties.WORKER_ID.get())))
                .willReturn(
                        aResponse().withStatus(201)
                )
                .atPriority(Integer.MAX_VALUE)
        );

        LOG.info("{} Wiremock: Initialized.", logPrefix);
    }

    private void startWiremock() {
        wireMockServer = new WireMockServer(
                WireMockConfiguration.options().dynamicHttpsPort().dynamicPort()
        );
        wireMockServer.start();
        resetWiremock();
    }

    public String getWiremockUrl() {
        return getWiremockUrl(false);
    }

    public String getWiremockUrl(boolean https) {
        String url;
        if (!https) {
            url = new HttpUrl.Builder()
                    .scheme("http")
                    .port(wireMockServer.port())
                    .host("localhost")
                    .build()
                    .toString();
        } else {
            url = new HttpUrl.Builder()
                    .scheme("https")
                    .port(wireMockServer.httpsPort())
                    .host("localhost")
                    .build()
                    .toString();
        }

        // no / at the end..
        if (url.endsWith("/")) {
            url = url.substring(0, url.length()-1);
        }

        return url;
    }

    private void startRabbitMq() {
        rabbitMQContainer = new RabbitMQContainer(rabbitmqImage).withAdminPassword(null);
        rabbitmqResourcesClassPathMapping.forEach((k, v) -> {
            rabbitMQContainer = rabbitMQContainer.withClasspathResourceMapping(k, v, BindMode.READ_WRITE);
        });

        if (configureRabbitMQContainer != null) {
            rabbitMQContainer = configureRabbitMQContainer.configure(rabbitMQContainer);
        }

        rabbitMQContainer.start();

        WorkerProperties.setOverride("queue.host", String.valueOf(rabbitMQContainer.getHost()));
        WorkerProperties.setOverride("queue.port", String.valueOf(rabbitMQContainer.getAmqpPort()));

        LOG.info("{} RabbitMQ is up at {}", logPrefix, rabbitMQContainer.getAmqpUrl());
    }

    private void startMongoDb() {
        mongoDBContainer = new MongoDBContainer(DockerImageName.parse(mongodbImage));
        mongoDbResourcesClassPathMapping.forEach((k, v) -> {
            mongoDBContainer = mongoDBContainer.withClasspathResourceMapping(k, v, BindMode.READ_WRITE);
        });

        if (configureMongoDBContainer != null) {
            mongoDBContainer = configureMongoDBContainer.configure(mongoDBContainer);
        }

        mongoDBContainer.start();

        LOG.info("{} MongoDB is up at {}", logPrefix, mongoDBContainer.getConnectionString());
    }

}
