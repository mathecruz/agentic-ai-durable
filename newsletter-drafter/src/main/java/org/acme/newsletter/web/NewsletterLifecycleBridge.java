package org.acme.newsletter.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.CompletionStage;
import org.acme.newsletter.persistence.Newsletter;
import org.acme.newsletter.persistence.NewsletterStore;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listens to the engine lifecycle events (topic "flow-lifecycle-out") of the newsletter workflow, keeps the
 * {@link Newsletter} read model up to date and pushes the progress to the WebSocket clients at /ws/newsletter.
 */
@ApplicationScoped
public class NewsletterLifecycleBridge {

    private static final Logger LOG = LoggerFactory.getLogger(NewsletterLifecycleBridge.class);

    private static final String WORKFLOW_NAME = "intelligent-drafter";
    private static final String TASK_STARTED_TYPE = "io.serverlessworkflow.task.started.v1";
    private static final String TASK_COMPLETED_TYPE = "io.serverlessworkflow.task.completed.v1";
    private static final String STATUS_CHANGED_TYPE = "io.serverlessworkflow.workflow.status-changed.v1";

    @Inject
    ObjectMapper objectMapper;

    @Inject
    NewsletterStore store;

    // the WebSocket broadcast uses the blocking basic remote, which is not allowed on the event loop
    @Incoming("flow-lifecycle-incoming")
    @Blocking
    public CompletionStage<Void> onLifecycleEvent(Message<String> msg) {
        try {
            CloudEventMetadata<?> ce = msg.getMetadata(CloudEventMetadata.class).orElse(null);
            String payload = msg.getPayload();
            if (ce == null || ce.getType() == null || payload == null || payload.isEmpty())
                return msg.ack();

            JsonNode data = objectMapper.readTree(payload);
            // the agentic sub-workflow runs as a separate instance, only the newsletter workflow is tracked
            if (!WORKFLOW_NAME.equals(data.path("definition").path("name").asText()))
                return msg.ack();

            switch (ce.getType()) {
                case TASK_STARTED_TYPE -> onTask(data, "started");
                case TASK_COMPLETED_TYPE -> onTask(data, "completed");
                case STATUS_CHANGED_TYPE -> onStatusChanged(data);
                default -> {
                }
            }
        } catch (Exception ex) {
            LOG.error("Failed to consume lifecycle event {}", msg.getPayload(), ex);
        }
        return msg.ack();
    }

    private void onTask(JsonNode data, String phase) throws Exception {
        String instanceId = data.path("workflow").asText(null);
        // task is a JSON pointer such as "do/2/waitHumanReview"
        String task = data.path("task").asText("");
        String step = task.substring(task.lastIndexOf('/') + 1);
        if (instanceId == null || step.isEmpty())
            return;

        if ("started".equals(phase)) {
            store.stepStarted(instanceId, step);
        }

        ObjectNode envelope = objectMapper.createObjectNode();
        envelope.put("type", "progress");
        envelope.put("instanceId", instanceId);
        envelope.put("step", step);
        envelope.put("phase", phase);
        envelope.put("at", data.path("started".equals(phase) ? "startedAt" : "completedAt").asText(null));
        NewsletterUpdatesSocket.broadcast(objectMapper.writeValueAsString(envelope));
    }

    private void onStatusChanged(JsonNode data) throws Exception {
        String instanceId = data.path("name").asText(null);
        Newsletter.Status status = switch (data.path("status").asText("")) {
            case "COMPLETED" -> Newsletter.Status.SENT;
            case "FAULTED" -> Newsletter.Status.FAILED;
            case "CANCELLED" -> Newsletter.Status.CANCELLED;
            default -> null;
        };
        if (instanceId == null || status == null)
            return;

        store.finished(instanceId, status);

        ObjectNode envelope = objectMapper.createObjectNode();
        envelope.put("type", "status");
        envelope.put("instanceId", instanceId);
        envelope.put("status", status.name());
        NewsletterUpdatesSocket.broadcast(objectMapper.writeValueAsString(envelope));
    }
}
