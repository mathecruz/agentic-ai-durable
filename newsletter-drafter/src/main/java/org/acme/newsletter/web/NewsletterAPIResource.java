package org.acme.newsletter.web;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.acme.newsletter.NewsletterWorkflow;
import org.acme.newsletter.domain.HumanReview;
import org.acme.newsletter.domain.NewsletterRequest;
import org.acme.newsletter.persistence.Newsletter;
import org.acme.newsletter.persistence.NewsletterStore;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.serverlessworkflow.impl.WorkflowInstance;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;

@Path("/api")
public class NewsletterAPIResource {

    @Inject
    NewsletterWorkflow newsletterWorkflow;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    NewsletterStore store;

    // Kafka producer bound to topic `flow-in`
    @Inject
    @Channel("flow-in-outgoing")
    Emitter<String> flowIn;

    /**
     * Starts the workflow to create a new newsletter draft.
     *
     * @param request input from the user
     * @return A workflow instance that will call the agents and produce a request for review event once it's done.
     */
    @POST
    @Path("/newsletter")
    @Blocking
    public Response newNewsletter(NewsletterRequest request) {
        final WorkflowInstance instance = newsletterWorkflow.instance(request);
        // registered before starting, so the workflow events always find it
        store.create(instance.id(), request);
        // fire and forget (agents will be called on a thread within the engine)
        CompletableFuture.runAsync(instance::start);
        return Response.accepted(Map.of("instanceId", instance.id())).build();
    }

    @PUT
    @Path("/newsletter")
    @Blocking
    public Response sendReview(HumanReview review, @HeaderParam("X-Flow-Instance-Id") String instanceId)
            throws JsonProcessingException {
        if (instanceId == null || instanceId.isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST).entity("Missing X-Flow-Instance-Id header").build();
        }

        if (store.find(instanceId).isPresent() && !store.reviewed(instanceId, review)) {
            return Response.status(Response.Status.CONFLICT).entity("Newsletter is not waiting for a review").build();
        }

        String body = objectMapper.writeValueAsString(review);

        // CloudEvent attributes are sent as Kafka ce_* headers (binary mode); the payload is the event data
        OutgoingCloudEventMetadata<?> ce = OutgoingCloudEventMetadata.builder().withId(UUID.randomUUID().toString())
                .withSource(URI.create("api:/newsletter")).withType("org.acme.newsletter.review.done")
                .withDataContentType("application/json")
                // correlates the review with the waiting workflow instance
                .withExtension("flowinstanceid", instanceId).build();

        flowIn.send(Message.of(body).addMetadata(ce));

        return Response.accepted().build();
    }

    @GET
    @Path("/newsletters")
    @Blocking
    public List<Newsletter> listNewsletters(@QueryParam("limit") Integer limit) {
        return store.latest(limit == null ? 50 : Math.max(1, Math.min(limit, 200)));
    }

    @GET
    @Path("/newsletters/{instanceId}")
    @Blocking
    public Response getNewsletter(@PathParam("instanceId") String instanceId) {
        return store.find(instanceId).map(n -> Response.ok(n).build())
                .orElse(Response.status(Response.Status.NOT_FOUND).build());
    }

}
