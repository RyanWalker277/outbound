package com.uci.outbound.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.uci.outbound.consumers.OutboundKafkaController;
import com.uci.outbound.model.MessageRequest;
import com.uci.utils.BotService;
import com.uci.utils.model.ApiResponse;
import com.uci.utils.model.ApiResponseParams;
import com.uci.utils.model.HttpApiResponse;
import lombok.extern.slf4j.Slf4j;
import messagerosa.core.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import javax.ws.rs.BadRequestException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;

@Slf4j
@RestController
@RequestMapping(value = "/message")
public class MessageController {
    @Autowired
    public BotService botService;

    @Autowired
    public OutboundKafkaController outboundService;

    @RequestMapping(value = "/send", method = RequestMethod.POST, produces = {"application/json", "text/json"})
    public Mono<ResponseEntity<HttpApiResponse>> sendMessage(@RequestBody MessageRequest request) {
        HttpApiResponse response = HttpApiResponse.builder()
                .status(HttpStatus.OK.value())
                .path("/message/send")
                .build();
        if(request.getAdapterId() == null || request.getAdapterId().isEmpty()
            || request.getTo() == null || request.getTo().getUserID() == null
                || request.getTo().getUserID().isEmpty() || request.getTo().getDeviceType() == null || request.getPayload() == null
        ) {
            response.setStatus(HttpStatus.BAD_REQUEST.value());
            response.setError(HttpStatus.BAD_REQUEST.name());
            response.setMessage("Adapter id, to with userID, deviceType & payload are required.");
            return Mono.just(ResponseEntity.badRequest().body(response));
        } else if(request.getPayload().getText() == null && request.getPayload().getMedia() == null) {
            response.setStatus(HttpStatus.BAD_REQUEST.value());
            response.setError(HttpStatus.BAD_REQUEST.name());
            response.setMessage("Payload should have either text or media.");
            return Mono.just(ResponseEntity.badRequest().body(response));
        } else if(request.getPayload().getMedia() != null
                && (request.getPayload().getMedia().getUrl() == null || request.getPayload().getMedia().getCategory() == null)
        ) {
            response.setStatus(HttpStatus.BAD_REQUEST.value());
            response.setError(HttpStatus.BAD_REQUEST.name());
            response.setMessage("Payload media should have category and url.");
            return Mono.just(ResponseEntity.badRequest().body(response));
        } else {
            SenderReceiverInfo from = new SenderReceiverInfo().builder().userID("admin").build();
            SenderReceiverInfo to = request.getTo();
            MessageId msgId = new MessageId().builder().channelMessageId(UUID.randomUUID().toString()).replyId(to.getUserID()).build();
            XMessagePayload payload = request.payload;

            return botService.getAdapterByID(request.getAdapterId())
                    .map(new Function<JsonNode, ResponseEntity<HttpApiResponse>>(){
                        @Override
                        public ResponseEntity<HttpApiResponse> apply(JsonNode adapter) {
                            XMessage xmsg = new XMessage().builder()
                                    .app("Global Outbound Bot")
                                    .adapterId(request.getAdapterId())
                                    .sessionId(UUID.randomUUID())
                                    .ownerId(null)
                                    .ownerOrgId(null)
                                    .from(from)
                                    .to(to)
                                    .messageId(msgId)
                                    .messageState(XMessage.MessageState.REPLIED)
                                    .messageType(XMessage.MessageType.TEXT)
                                    .payload(payload)
                                    .providerURI(adapter.path("provider").asText())
                                    .channelURI(adapter.path("channel").asText())
                                    .timestamp(Timestamp.valueOf(LocalDateTime.now()).getTime())
                                    .build();

                            /**
                             * Check for media content allowed for gupshup & netcore whatsapp adapter
                             */
                            if(request.getPayload().getMedia() != null
                                    && !adapter.path("channel").asText().equalsIgnoreCase("whatsapp")
                            ) {
                                response.setStatus(HttpStatus.BAD_REQUEST.value());
                                response.setError(HttpStatus.BAD_REQUEST.name());
                                response.setMessage("Media is allowed only for gupshup whatsapp & netcore whatsapp adapter.");
                                return ResponseEntity.badRequest().body(response);
                            }

                            /* Template id required check for cdac sms adapter */
                            if(adapter.path("channel").asText().equalsIgnoreCase("sms")
                                    &&  adapter.path("provider").asText().equalsIgnoreCase("cdac")) {
                                if(request.getTo().getMeta() == null || request.getTo().getMeta().get("templateId") == null || request.getTo().getMeta().get("templateId").isEmpty()) {
                                    response.setStatus(HttpStatus.BAD_REQUEST.value());
                                    response.setError(HttpStatus.BAD_REQUEST.name());
                                    response.setMessage("Template id in meta of to is required for firebase adapter messaging.");
                                    return ResponseEntity.badRequest().body(response);
                                } else {
                                    HashMap<String, String> transformerMeta = new HashMap<>();
                                    transformerMeta.put("templateId", request.getTo().getMeta().get("templateId"));
                                    Transformer transformer = Transformer.builder().metaData(transformerMeta).build();
                                    ArrayList<Transformer> transformers = new ArrayList<>();
                                    transformers.add(transformer);

                                    xmsg.setTransformers(transformers);
                                }
                            }

                            /* FCM token required check for firebase adapter */
                            if(adapter.path("channel").asText().equalsIgnoreCase("web")
                                    &&  adapter.path("provider").asText().equalsIgnoreCase("firebase")
                                    && (request.getTo().getMeta() == null || request.getTo().getMeta().get("fcmToken") == null || request.getTo().getMeta().get("fcmToken").isEmpty())) {
                                response.setStatus(HttpStatus.BAD_REQUEST.value());
                                response.setError(HttpStatus.BAD_REQUEST.name());
                                response.setMessage("FCM token in meta of to is required for firebase adapter messaging.");
                                return ResponseEntity.badRequest().body(response);
                            }

                            try {
                                outboundService.sendOutboundMessage(xmsg);
                                response.setMessage("Message processed.");
                            } catch (Exception e) {
                                log.error("Exception while sending outbound message: "+e.getMessage());
                            }

                            return ResponseEntity.ok(response);
                        }
                    });
        }
    }
}
