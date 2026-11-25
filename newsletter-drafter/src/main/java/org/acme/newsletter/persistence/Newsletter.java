package org.acme.newsletter.persistence;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import java.time.Instant;

/**
 * Read model of a newsletter workflow instance, used by the UI to list and resume drafts. The workflow state itself is
 * persisted by Quarkus Flow; this table only tracks what the UI needs.
 */
@Entity
public class Newsletter extends PanacheEntityBase {

    public enum Status {
        DRAFTING,
        AWAITING_REVIEW,
        REVISING,
        SENDING,
        SENT,
        FAILED,
        CANCELLED;

        public boolean isFinal() {
            return this == SENT || this == FAILED || this == CANCELLED;
        }
    }

    @Id
    public String instanceId;

    @Enumerated(EnumType.STRING)
    public Status status;

    public String currentStep;

    public String mood;
    public String tone;
    public String length;
    public String topMovers;

    @Column(columnDefinition = "text")
    public String macroData;

    @Column(columnDefinition = "text")
    public String title;

    @Column(columnDefinition = "text")
    public String lead;

    @Column(columnDefinition = "text")
    public String body;

    @Column(columnDefinition = "text")
    public String notes;

    public Instant createdAt;
    public Instant updatedAt;
}
