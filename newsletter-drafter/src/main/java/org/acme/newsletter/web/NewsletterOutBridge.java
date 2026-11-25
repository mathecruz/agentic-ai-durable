package org.acme.newsletter.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.CompletionStage;
import org.acme.newsletter.domain.NewsletterDraft;
import org.acme.newsletter.persistence.NewsletterStore;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listens to Kafka topic "flow-out" and forwards only the critic "review required" events to all connected WebSocket
 * clients at /ws/newsletter.
 */
@ApplicationScoped
public class NewsletterOutBridge {

    private static final Logger LOG = LoggerFactory.getLogger(NewsletterOutBridge.class);

    // match the type emitted by our workflow: "org.acme.email.review.required"
    private static final String REVIEW_REQUIRED_TYPE = "org.acme.email.review.required";

    @Inject
    ObjectMapper objectMapper;

    @Inject
    NewsletterStore store;

    // the WebSocket broadcast uses the blocking basic remote, which is not allowed on the event loop
    @Incoming("flow-out-incoming")
    @Blocking
    public CompletionStage<Void> onFlowOut(Message<String> msg) {
        try {
            // CloudEvent attributes travel as Kafka ce_* headers (binary mode); the payload is the event data
            CloudEventMetadata<?> ce = msg.getMetadata(CloudEventMetadata.class).orElse(null);
            if (ce == null || ce.getType() == null)
                return msg.ack();

            if (REVIEW_REQUIRED_TYPE.equals(ce.getType())) {
                String instanceId = ce.<String> getExtension("flowinstanceid").orElse(null);
                String data = msg.getPayload();

                if (instanceId != null && data != null && !data.isEmpty()) {
                    store.awaitingReview(instanceId, objectMapper.readValue(data, NewsletterDraft.class));
                }

                // Envelope with the workflow instance id, so each browser only picks up its own draft.
                ObjectNode envelope = objectMapper.createObjectNode();
                envelope.put("type", REVIEW_REQUIRED_TYPE);
                envelope.put("instanceId", instanceId);
                envelope.set("data", data == null || data.isEmpty() ? null : objectMapper.readTree(data));
                String json = objectMapper.writeValueAsString(envelope);

                LOG.info("Received review (workflow instance: {}) required event: {}", instanceId, json);

                NewsletterUpdatesSocket.broadcast(json);
            }
        } catch (Exception ex) {
            LOG.error("Failed to consume event {}", msg.getPayload(), ex);
        }
        return msg.ack();
    }
}
