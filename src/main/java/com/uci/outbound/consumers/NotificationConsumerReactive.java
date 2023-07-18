package com.uci.outbound.consumers;

import com.uci.adapter.provider.factory.IProvider;
import com.uci.adapter.provider.factory.ProviderFactory;
import com.uci.dao.models.XMessageDAO;
import com.uci.dao.repository.XMessageRepository;
import com.uci.dao.utils.XMessageDAOUtils;
import com.uci.utils.cache.service.RedisCacheService;
import com.uci.utils.model.EmailDetails;
import com.uci.utils.service.EmailServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import messagerosa.core.model.XMessage;
import messagerosa.xml.XMessageParser;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationConsumerReactive {
    @Autowired
    private EmailServiceImpl emailService;
    @Value("${spring.mail.recipient}")
    private String recipient;
    @Autowired
    private ProviderFactory factoryProvider;
    @Autowired
    private RedisCacheService redisCacheService;
    @Autowired
    private XMessageRepository xMessageRepo;

    private final Flux<ReceiverRecord<String, String>> reactiveKafkaReceiverNotification;

    private long notificationCount, otherCount, consumeCount;

    private final String emailSubject = "Error in Notification Consumer";

    @Value("${notification.bufferMaxSize}")
    private int notificationBufferMaxSize;
    @Value("${notification.bufferMaxTime}")
    private long notificationBufferMaxTime;
    @Value("${cassandra.bufferMaxSize}")
    private int cassandraBufferMaxSize;
    @Value("${cassandra.bufferMaxTime}")
    private long cassandraBufferMaxTime;


    @EventListener(ApplicationStartedEvent.class)
    public void onMessage() {
        try {
            reactiveKafkaReceiverNotification
                    .doOnNext(this::logMessage)
                    .bufferTimeout(notificationBufferMaxSize, Duration.ofSeconds(notificationBufferMaxTime))
                    .flatMap(this::sendOutboundMessage)
                    .onBackpressureBuffer()
                    .bufferTimeout(cassandraBufferMaxSize, Duration.ofSeconds(cassandraBufferMaxTime))
                    .flatMap(this::persistToCassandra)
                    .doOnError(this::handleKafkaFluxError)
                    .subscribe();
        } catch (Exception ex) {
            log.error("NotificationConsumerReactive:Exception: Exception: " + ex.getMessage());
        }
    }

    private void logMessage(ReceiverRecord<String, String> msg) {
        consumeCount++;
        log.info("NotificationConsumerReactive:Notification topic consume from kafka: " + consumeCount);
    }

    public Flux<XMessage> sendOutboundMessage(List<ReceiverRecord<String, String>> msgs) {
        return Flux.fromIterable(msgs)
                .flatMap(record -> {
                    try {
                        XMessage currentXmsg = XMessageParser.parse(new ByteArrayInputStream(record.value().getBytes()));
                        return Mono.just(currentXmsg);
                    } catch (Exception ex) {
                        log.error("NotificationConsumerReactive:Exception: " + ex.getMessage());
                        return Mono.empty();
                    }
                })
                .collectList()
                .flatMapMany(xMessageList -> {
                    String channel = "web";
                    String provider = "firebase";
                    try {
                        IProvider iprovider = factoryProvider.getProvider(provider, channel);
                        return iprovider.processOutBoundMessageF(Mono.just(xMessageList))
                                .onErrorResume(e -> {
                                    HashMap<String, String> attachments = new HashMap<>();
                                    attachments.put("Exception", ExceptionUtils.getStackTrace(e));
                                    sentEmail(null, "PFA", null, attachments);
                                    log.error("NotificationConsumerReactive:Exception: Exception in processOutBoundMessageF:" + e.getMessage());
                                    return Flux.empty();
                                });
                    } catch (Exception e) {
                        HashMap<String, String> attachments = new HashMap<>();
                        attachments.put("Exception", ExceptionUtils.getStackTrace(e));
                        attachments.put("XMessage", "No xmessage");
                        sentEmail(null, "PFA", null, attachments);
                        log.error("NotificationConsumerReactive:Exception: " + e.getMessage());
                        return Flux.empty();
                    }
                });
    }

    public Flux<XMessage> persistToCassandra(List<XMessage> xMessageList) {
        return Flux.fromIterable(xMessageList)
                .flatMap(this::saveXMessage)
                .doOnError(this::handlePersistToCassandraError);
    }


    public Mono<XMessage> saveXMessage(XMessage xMessage) {
        if (xMessage.getApp() != null) {
            try {
//                log.info("NotificationConsumerReactive:saveXMessage::convertXMessageToDAO : " + xMessage.toString());
                XMessageDAO dao = null;
                dao = XMessageDAOUtils.convertXMessageToDAO(xMessage);
                redisCacheService.setXMessageDaoCache(xMessage.getTo().getUserID(), dao);
                xMessageRepo
                        .insert(dao)
                        .doOnError(new Consumer<Throwable>() {
                            @Override
                            public void accept(Throwable e) {
                                redisCacheService.deleteXMessageDaoCache(xMessage.getTo().getUserID());
                                log.error("NotificationConsumerReactive:Notification Not Inserted in Cass: Exception: " + e.getMessage());
                            }
                        })
                        .subscribe(new Consumer<XMessageDAO>() {
                            @Override
                            public void accept(XMessageDAO xMessageDAO) {
                                log.info("NotificationConsumerReactive: XMessage Object saved is with sent user ID >> " + xMessageDAO.getUserId());

                                String channel = xMessage.getChannelURI();
                                String provider = xMessage.getProviderURI();

                                if (provider.toLowerCase().equals("firebase") && channel.toLowerCase().equals("web")) {
                                    notificationCount++;
                                    log.info("NotificationConsumerReactive:Notification Insert Record in Cass : BotId : " + xMessageDAO.getBotUuid() + " count: " + notificationCount);
//                                    logTimeTaken(startTime, 0, "NotificationConsumerReactive:Notification Insert Record in Cass : " + notificationCount + " ::: process-end: %d ms");
                                } else {
                                    otherCount++;
//                                    logTimeTaken(startTime, 0, "Other Insert Record in Cass : " + otherCount + " ::: process-end: %d ms");
                                    log.info("Other Insert Record in Cass : " + otherCount);
                                }
                            }
                        });
            } catch (Exception e) {
                HashMap<String, String> attachments = new HashMap<>();
                attachments.put("Exception", ExceptionUtils.getStackTrace(e));
                attachments.put("XMessage", xMessage.toString());
                sentEmail(xMessage, "PFA", null, attachments);
                log.error("NotificationConsumerReactive:Exception: Exception in convertXMessageToDAO: " + e.getMessage());
            }
        } else {
            log.info("NotificationConsumerReactive:XMessage -> app is empty " + xMessage);
        }
        return Mono.empty();
    }

    private void handleKafkaFluxError(Throwable e) {
        HashMap<String, String> attachments = new HashMap<>();
        attachments.put("Exception", ExceptionUtils.getStackTrace(e));
        try {
            sentEmail(null, "PFA", null, attachments);
        } catch (Exception ex) {
            log.error("NotificationConsumerReactive:Exception:" + ex.getMessage());
        }
        log.error("NotificationConsumerReactive:Exception: " + e.getMessage());
    }

    public void handlePersistToCassandraError(Throwable throwable) {
        log.error("NotificationConsumerReactive:Exception: Error in persistToCassandra: " + throwable.getMessage());
    }

    private void sentEmail(XMessage xMessage, String body, String attachmentFileName, HashMap<String, String> attachments) {
        log.info("Email Sending....");
        EmailDetails emailDetails = new EmailDetails().builder()
                .subject(emailSubject)
                .msgBody(body)
                .recipient(recipient)
                .attachment(xMessage == null ? "" : xMessage.toString())
                .attachmentFileName(attachmentFileName)
                .attachments(attachments)
                .build();
        emailService.sendMailWithAttachment(emailDetails);
    }
}
