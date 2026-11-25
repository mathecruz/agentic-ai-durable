package org.acme.newsletter.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.panache.common.Page;
import io.quarkus.panache.common.Sort;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.acme.newsletter.domain.HumanReview;
import org.acme.newsletter.domain.NewsletterDraft;
import org.acme.newsletter.domain.NewsletterRequest;
import org.acme.newsletter.persistence.Newsletter.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps the {@link Newsletter} read model in sync with the workflow. Updates may arrive more than once (every replica
 * consumes the workflow events), so they must be idempotent.
 */
@ApplicationScoped
public class NewsletterStore {

    private static final Logger LOG = LoggerFactory.getLogger(NewsletterStore.class);
    private static final String WORKFLOW_NAME = "intelligent-drafter";

    @Inject
    ObjectMapper objectMapper;

    @Transactional
    public void create(String instanceId, NewsletterRequest request) {
        Newsletter n = new Newsletter();
        n.instanceId = instanceId;
        n.status = Status.DRAFTING;
        applyRequest(n, request);
        n.createdAt = Instant.now();
        n.updatedAt = n.createdAt;
        n.persist();
    }

    /**
     * Registers the active instances started before this read model existed (or before a crash), using the instances
     * persisted by Quarkus Flow. Finished instances are no longer there, so they are never brought back.
     */
    @Transactional
    void backfill(@Observes StartupEvent event) {
        @SuppressWarnings("unchecked")
        List<String> missing = Newsletter.getEntityManager().createNativeQuery(
                "SELECT w.instance_id FROM workflow_instance_entity w WHERE w.workflow_name = :name"
                        + " AND NOT EXISTS (SELECT 1 FROM newsletter n WHERE n.instance_id = w.instance_id)")
                .setParameter("name", WORKFLOW_NAME).getResultList();
        missing.forEach(this::fromEngine);
        if (!missing.isEmpty()) {
            LOG.info("Registered {} newsletter(s) from running workflow instances: {}", missing.size(), missing);
        }
    }

    private void applyRequest(Newsletter n, NewsletterRequest request) {
        if (request == null)
            return;
        n.mood = request.mood() != null ? request.mood().name() : null;
        n.tone = request.tone() != null ? request.tone().name() : null;
        n.length = request.length() != null ? request.length().name() : null;
        n.topMovers = request.topMovers() != null ? String.join(", ", request.topMovers()) : null;
        n.macroData = request.macroData();
    }

    /**
     * Creates the newsletter of a workflow instance that is still persisted by Quarkus Flow, if any.
     */
    private Optional<Newsletter> fromEngine(String instanceId) {
        // idempotent: replicas and both event consumers may register the same instance concurrently
        int inserted = Newsletter.getEntityManager().createNativeQuery(
                "INSERT INTO newsletter (instance_id, status, created_at, updated_at)"
                        + " SELECT instance_id, 'DRAFTING', started_at, now() FROM workflow_instance_entity"
                        + " WHERE instance_id = :id AND workflow_name = :name ON CONFLICT (instance_id) DO NOTHING")
                .setParameter("id", instanceId).setParameter("name", WORKFLOW_NAME).executeUpdate();

        Optional<Newsletter> newsletter = find(instanceId);
        if (inserted > 0) {
            @SuppressWarnings("unchecked")
            List<Object> input = Newsletter.getEntityManager().createNativeQuery(
                    "SELECT input FROM workflow_instance_entity WHERE instance_id = :id")
                    .setParameter("id", instanceId).getResultList();
            newsletter.ifPresent(n -> applyRequest(n, input.isEmpty() ? null : readRequest(input.get(0))));
        }
        return newsletter;
    }

    /** Best effort: the engine stores the input as a serialized model wrapping the request JSON. */
    private NewsletterRequest readRequest(Object input) {
        if (!(input instanceof byte[] bytes))
            return null;
        String raw = new String(bytes, StandardCharsets.UTF_8);
        int start = raw.indexOf('{');
        int end = raw.lastIndexOf('}');
        if (start < 0 || end < start)
            return null;
        try {
            return objectMapper.readValue(raw.substring(start, end + 1), NewsletterRequest.class);
        } catch (Exception e) {
            LOG.debug("Unable to read the request of a persisted workflow instance", e);
            return null;
        }
    }

    /** The newsletter of an instance, created from the engine state when it was started before this read model. */
    private Optional<Newsletter> findOrFromEngine(String instanceId) {
        Optional<Newsletter> newsletter = find(instanceId);
        return newsletter.isPresent() ? newsletter : fromEngine(instanceId);
    }

    @Transactional
    public void awaitingReview(String instanceId, NewsletterDraft draft) {
        // only while the agents are working: a late duplicate must not reopen a reviewed draft
        findOrFromEngine(instanceId).filter(n -> n.status == Status.DRAFTING || n.status == Status.REVISING).ifPresent(n -> {
            n.status = Status.AWAITING_REVIEW;
            setDraft(n, draft);
            n.updatedAt = Instant.now();
        });
    }

    /**
     * @return false if the newsletter does not exist or is not waiting for a review
     */
    @Transactional
    public boolean reviewed(String instanceId, HumanReview review) {
        Optional<Newsletter> newsletter = find(instanceId).filter(n -> n.status == Status.AWAITING_REVIEW);
        newsletter.ifPresent(n -> {
            n.status = review.status() == HumanReview.ReviewStatus.NEEDS_REVISION ? Status.REVISING : Status.SENDING;
            n.notes = review.notes();
            setDraft(n, review.draft());
            n.updatedAt = Instant.now();
        });
        return newsletter.isPresent();
    }

    @Transactional
    public void stepStarted(String instanceId, String step) {
        findOrFromEngine(instanceId).filter(n -> !n.status.isFinal()).ifPresent(n -> {
            n.currentStep = step;
            n.updatedAt = Instant.now();
        });
    }

    @Transactional
    public void finished(String instanceId, Status status) {
        find(instanceId).filter(n -> !n.status.isFinal()).ifPresent(n -> {
            n.status = status;
            n.updatedAt = Instant.now();
        });
    }

    public Optional<Newsletter> find(String instanceId) {
        return Newsletter.findByIdOptional(instanceId);
    }

    public List<Newsletter> latest(int limit) {
        return Newsletter.findAll(Sort.descending("createdAt")).page(Page.ofSize(limit)).list();
    }

    private static void setDraft(Newsletter n, NewsletterDraft draft) {
        if (draft == null)
            return;
        n.title = draft.title();
        n.lead = draft.lead();
        n.body = draft.body();
    }
}
