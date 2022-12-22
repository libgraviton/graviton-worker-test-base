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
import org.junit.jupiter.api.extension.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

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

    /**
     * mongodb
     */
    private boolean startMongodb = false;
    private String mongodbImage = "mongo:6.0";
    private final HashMap<String, String> mongoDbResourcesClassPathMapping = new HashMap<>();

    /**
     * rabbitmq
     */
    private boolean startRabbitMq = false;
    private String rabbitmqImage = "rabbitmq:3-management";
    private final HashMap<String, String> rabbitmqResourcesClassPathMapping = new HashMap<>();

    protected static ObjectMapper objectMapper;

    protected static WireMockServer wireMockServer;
    protected static RabbitMQContainer rabbitMQContainer;
    protected static MongoDBContainer mongoDBContainer;
    protected static QueueManager queueManager;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        WorkerProperties.load();
        DependencyInjection.init();

        objectMapper = DependencyInjection.getInstance(ObjectMapper.class);

        if (startWiremock) {
            startWiremock();
            WorkerProperties.setOverride(WorkerProperties.GRAVITON_BASE_URL.toString(), wireMockServer.baseUrl());
        }
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        if (wireMockServer != null) {
            wireMockServer.stop();
            wireMockServer = null;
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        if (startRabbitMq) {
            startRabbitMq();
            queueManager = DependencyInjection.getInstance(QueueManager.class);
        }
        if (startMongodb) {
            startMongoDb();
        }
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        WorkerProperties.clearOverrides();

        if (rabbitMQContainer != null) {
            rabbitMQContainer.stop();
            rabbitMQContainer = null;
        }
        if (mongoDBContainer != null) {
            mongoDBContainer.stop();
            mongoDBContainer = null;
        }
        if (wireMockServer != null) {
            resetWiremock();
        }
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

    public CountDownLatch getCountDownLatch(int countdown, WorkerLauncher launcher) {
        final CountDownLatch countDownLatch = new CountDownLatch(countdown);
        launcher.getQueueWorkerRunner().addOnCompleteCallback((duration) -> {
            countDownLatch.countDown();
        });
        return countDownLatch;
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

        LOG.info("********* EVENT STATUS URL {}", eventStatusUrl);

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
    }

    private void resetWiremock() {
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
    }

    private void startWiremock() {
        wireMockServer = new WireMockServer(WireMockConfiguration.options().port(8080)); //No-args constructor will start on port 8080, no HTTPS
        wireMockServer.start();
        resetWiremock();
    }

    private void startRabbitMq() {
        rabbitMQContainer = new RabbitMQContainer(rabbitmqImage).withAdminPassword(null);
        rabbitmqResourcesClassPathMapping.forEach((k, v) -> {
            rabbitMQContainer = rabbitMQContainer.withClasspathResourceMapping(k, v, BindMode.READ_WRITE);
        });
        rabbitMQContainer.start();

        WorkerProperties.setOverride("queue.port", String.valueOf(rabbitMQContainer.getAmqpPort()));
    }

    private void startMongoDb() {
        mongoDBContainer = new MongoDBContainer(DockerImageName.parse(mongodbImage));
        mongoDbResourcesClassPathMapping.forEach((k, v) -> {
            mongoDBContainer = mongoDBContainer.withClasspathResourceMapping(k, v, BindMode.READ_WRITE);
        });
        mongoDBContainer.start();
    }

}
