package org.example.exactlyonce.orchestrator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.server.ResponseStatusException;

@SpringBootApplication
public class OrchestratorApplication {
  public static void main(String[] args) {
    SpringApplication.run(OrchestratorApplication.class, args);
  }
}

record InvocationRequest(
    String experimentId,
    String runId,
    long seed,
    String variant,
    String downstreamCapability,
    String failureScenario,
    String ledgerFailureMode,
    double failureProbability,
    int concurrency,
    String operationId,
    String attemptId,
    String toolName,
    String requestHash,
    JsonNode arguments,
    Integer leaseMs) {}

record InvocationResponse(
    String operationId,
    String attemptId,
    String status,
    String finalState,
    boolean replayed,
    boolean reconciled,
    String effectId,
    JsonNode result) {}

record LedgerDecision(String mode, InvocationResponse response) {}

record DownstreamRequest(String operationId, String attemptId, String requestHash, String capability, String experimentId, String runId, JsonNode arguments) {}
record DownstreamResult(String effectId, String status, boolean replayed, JsonNode result) {}
record LedgerRow(String operationId, String toolName, String requestHash, String state, String effectReference, JsonNode resultPayload, int attemptCount) {}

@RestController
class OrchestratorController {
  private final JdbcTemplate jdbc;
  private final RestTemplate http = new RestTemplate();
  private final ObjectMapper mapper = new ObjectMapper();
  private final FaultDecider faults = new FaultDecider();
  private final Map<String, String> serviceUrls;
  private final TransactionTemplate tx;

  OrchestratorController(
      JdbcTemplate jdbc,
      TransactionTemplate tx,
      @Value("${services.order}") String orderUrl,
      @Value("${services.payment}") String paymentUrl,
      @Value("${services.inventory}") String inventoryUrl,
      @Value("${services.notification}") String notificationUrl) {
    this.jdbc = jdbc;
    this.tx = tx;
    this.serviceUrls = Map.of(
        "create_order", orderUrl,
        "charge_payment", paymentUrl,
        "reserve_inventory", inventoryUrl,
        "send_notification", notificationUrl);
  }

  @GetMapping("/health")
  Map<String, String> health() {
    return Map.of("status", "ok", "service", "orchestrator");
  }

  @PostMapping("/invoke")
  InvocationResponse invoke(@RequestBody InvocationRequest request) throws JsonProcessingException {
    return switch (request.variant()) {
      case "V0" -> naive(request);
      case "V1" -> agentKeyOnly(request);
      case "V2" -> ledgerBacked(request, false, false);
      case "V3" -> downstreamIdempotent(request);
      case "V4" -> ledgerBacked(request, false, true);
      case "V5" -> ledgerBacked(request, true, true);
      default -> throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "UNKNOWN_VARIANT");
    };
  }

  private InvocationResponse naive(InvocationRequest request) {
    DownstreamResult result = dispatch(request, "C0");
    maybeLoseResponseAfterEffect(request);
    return new InvocationResponse(request.operationId(), request.attemptId(), "OK", "NO_LEDGER", false, false, result.effectId(), result.result());
  }

  private InvocationResponse agentKeyOnly(InvocationRequest request) {
    DownstreamResult result = dispatch(request, request.downstreamCapability());
    maybeLoseResponseAfterEffect(request);
    return new InvocationResponse(request.operationId(), request.attemptId(), "OK", "NO_LEDGER", result.replayed(), false, result.effectId(), result.result());
  }

  private InvocationResponse downstreamIdempotent(InvocationRequest request) {
    if (request.downstreamCapability().equals("C0")) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "V3_REQUIRES_C1_OR_C2");
    }
    DownstreamResult result = dispatch(request, request.downstreamCapability());
    maybeLoseResponseAfterEffect(request);
    return new InvocationResponse(request.operationId(), request.attemptId(), "OK", "SERVICE_DEDUPED", result.replayed(), false, result.effectId(), result.result());
  }

  private InvocationResponse ledgerBacked(InvocationRequest request, boolean allowReconcile, boolean propagateIdentity) throws JsonProcessingException {
    LedgerDecision decision = tx.execute(status -> prepareLedger(request, allowReconcile));
    if (!decision.mode().equals("PROCEED")) {
      if (decision.mode().equals("RECONCILE")) {
        return reconcile(request);
      }
      return decision.response();
    }

    if (faults.inject(request, "BEFORE_DOWNSTREAM_DISPATCH")) {
      tx.executeWithoutResult(status -> transition(request, "EXECUTING", "RETRYABLE_FAILURE", "timeout-before-downstream-dispatch"));
      return new InvocationResponse(request.operationId(), request.attemptId(), "RETRYABLE_FAILURE", "RETRYABLE_FAILURE", false, false, null, null);
    }

    DownstreamResult result = dispatch(request, propagateIdentity ? request.downstreamCapability() : "C0");

    if (faults.inject(request, "BEFORE_EFFECT_CONFIRMATION_PERSIST")) {
      if (request.failureScenario().equals("F8")) {
        return new InvocationResponse(request.operationId(), request.attemptId(), "CRASH_SIMULATED", "EXECUTING", false, false, result.effectId(), result.result());
      }
      tx.executeWithoutResult(status -> transition(request, "EXECUTING", "UNKNOWN", "ambiguous-after-downstream-dispatch"));
      return new InvocationResponse(request.operationId(), request.attemptId(), "UNKNOWN", "UNKNOWN", false, false, result.effectId(), result.result());
    }

    InvocationResponse response = tx.execute(status -> finalizeLedger(request, result));
    if (faults.inject(request, "AFTER_DOWNSTREAM_RESPONSE")) {
      sleepForDelayedResponse();
    }
    return response;
  }

  private InvocationResponse reconcile(InvocationRequest request) {
    if (!request.downstreamCapability().equals("C2")) {
      return new InvocationResponse(request.operationId(), request.attemptId(), "UNKNOWN", "UNKNOWN", false, false, null, null);
    }
    tx.executeWithoutResult(status -> transition(request, "UNKNOWN", "RECONCILING", "start-reconciliation"));
    try {
      ResponseEntity<DownstreamResult> response = http.getForEntity(serviceUrl(request.toolName()) + "/lookup/" + request.operationId(), DownstreamResult.class);
      DownstreamResult result = response.getBody();
      return tx.execute(status -> {
        try {
          persistEffectFromReconciling(request, result);
          transition(request, "EFFECT_CONFIRMED", "COMPLETED", "reconciled-effect");
          return new InvocationResponse(request.operationId(), request.attemptId(), "OK", "COMPLETED", true, true, result.effectId(), result.result());
        } catch (JsonProcessingException e) {
          throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "RESULT_SERIALIZATION_FAILURE", e);
        }
      });
    } catch (HttpClientErrorException.NotFound notFound) {
      tx.executeWithoutResult(status -> transition(request, "RECONCILING", "RETRYABLE_FAILURE", "reconciled-no-effect"));
      return new InvocationResponse(request.operationId(), request.attemptId(), "RETRYABLE_FAILURE", "RETRYABLE_FAILURE", false, true, null, null);
    } catch (RuntimeException e) {
      tx.executeWithoutResult(status -> transition(request, "RECONCILING", "UNKNOWN", "reconciliation-inconclusive"));
      return new InvocationResponse(request.operationId(), request.attemptId(), "UNKNOWN", "UNKNOWN", false, true, null, null);
    }
  }

  private LedgerDecision prepareLedger(InvocationRequest request, boolean allowReconcile) {
    if (faults.inject(request, "LEDGER_READ")) {
      throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "LEDGER_READ_UNAVAILABLE");
    }
    markExpiredExecutingUnknown(request);
    LedgerRow existing = findLedger(request.operationId());
    if (existing == null) {
      if (faults.inject(request, "LEDGER_WRITE")) {
        throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "LEDGER_WRITE_UNAVAILABLE");
      }
      insertLedger(request);
      existing = findLedger(request.operationId());
    } else if (!existing.requestHash().equals(request.requestHash())) {
      throw new ResponseStatusException(HttpStatus.CONFLICT, "OPERATION_ID_CONFLICT");
    }

    if (existing.state().equals("COMPLETED")) {
      return new LedgerDecision("RETURN", new InvocationResponse(request.operationId(), request.attemptId(), "OK", "COMPLETED", true, false, existing.effectReference(), existing.resultPayload()));
    }
    if (existing.state().equals("EFFECT_CONFIRMED")) {
      transition(request, "EFFECT_CONFIRMED", "COMPLETED", "recover-final-result");
      LedgerRow completed = findLedger(request.operationId());
      return new LedgerDecision("RETURN", new InvocationResponse(request.operationId(), request.attemptId(), "OK", "COMPLETED", true, false, completed.effectReference(), completed.resultPayload()));
    }
    if (existing.state().equals("EXECUTING") || existing.state().equals("CLAIMED")) {
      return new LedgerDecision("RETURN", new InvocationResponse(request.operationId(), request.attemptId(), "IN_PROGRESS", existing.state(), false, false, existing.effectReference(), existing.resultPayload()));
    }
    if (existing.state().equals("UNKNOWN")) {
      if (allowReconcile) {
        return new LedgerDecision("RECONCILE", null);
      }
      return new LedgerDecision("RETURN", new InvocationResponse(request.operationId(), request.attemptId(), "UNKNOWN", "UNKNOWN", false, false, existing.effectReference(), existing.resultPayload()));
    }
    if (existing.state().equals("FAILED_FINAL")) {
      return new LedgerDecision("RETURN", new InvocationResponse(request.operationId(), request.attemptId(), "FAILED_FINAL", "FAILED_FINAL", true, false, existing.effectReference(), existing.resultPayload()));
    }

    if (faults.inject(request, "LEDGER_WRITE")) {
      throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "LEDGER_WRITE_UNAVAILABLE");
    }
    claim(request, existing.state());
    transition(request, "CLAIMED", "EXECUTING", "begin-execution");
    return new LedgerDecision("PROCEED", null);
  }

  private InvocationResponse finalizeLedger(InvocationRequest request, DownstreamResult result) {
    try {
      persistEffect(request, result);
      if (faults.inject(request, "BEFORE_FINAL_RESULT_PERSIST")) {
        return new InvocationResponse(request.operationId(), request.attemptId(), "EFFECT_CONFIRMED", "EFFECT_CONFIRMED", false, false, result.effectId(), result.result());
      }
      transition(request, "EFFECT_CONFIRMED", "COMPLETED", "final-result-persisted");
      return new InvocationResponse(request.operationId(), request.attemptId(), "OK", "COMPLETED", false, false, result.effectId(), result.result());
    } catch (JsonProcessingException e) {
      throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "RESULT_SERIALIZATION_FAILURE", e);
    }
  }

  private DownstreamResult dispatch(InvocationRequest request, String capability) {
    DownstreamRequest body = new DownstreamRequest(request.operationId(), request.attemptId(), request.requestHash(), capability, request.experimentId(), request.runId(), request.arguments());
    return http.postForObject(serviceUrl(request.toolName()) + "/execute", body, DownstreamResult.class);
  }

  private void maybeLoseResponseAfterEffect(InvocationRequest request) {
    if (faults.inject(request, "BEFORE_EFFECT_CONFIRMATION_PERSIST")) {
      throw new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT, "RESPONSE_LOST_AFTER_EFFECT");
    }
  }

  private String serviceUrl(String toolName) {
    String url = serviceUrls.get(toolName);
    if (url == null) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "UNKNOWN_TOOL");
    return url;
  }

  private LedgerRow findLedger(String operationId) {
    List<LedgerRow> rows = jdbc.query(
        "SELECT operation_id, tool_name, request_hash, state::text, effect_reference, result_payload, attempt_count FROM runtime.execution_ledger WHERE operation_id = ? FOR UPDATE",
        (rs, rowNum) -> new LedgerRow(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), readTree(rs.getString(6)), rs.getInt(7)),
        operationId);
    return rows.isEmpty() ? null : rows.getFirst();
  }

  private void insertLedger(InvocationRequest request) {
    int inserted = jdbc.update("INSERT INTO runtime.execution_ledger(operation_id, tool_name, request_hash, state, variant, downstream_capability, last_attempt_id) VALUES (?, ?, ?, 'RECEIVED', ?, ?, ?) ON CONFLICT (operation_id) DO NOTHING",
        request.operationId(), request.toolName(), request.requestHash(), request.variant(), request.downstreamCapability(), request.attemptId());
    if (inserted == 1) {
      jdbc.update("INSERT INTO runtime.ledger_transitions(operation_id, state_before, state_after, attempt_id, reason) VALUES (?, NULL, 'RECEIVED', ?, 'receive')",
          request.operationId(), request.attemptId());
    }
  }

  private void claim(InvocationRequest request, String before) {
    String owner = UUID.randomUUID().toString();
    int leaseMs = request.leaseMs() == null ? 5000 : request.leaseMs();
    transition(request, before, "CLAIMED", "claim");
    jdbc.update("UPDATE runtime.execution_ledger SET owner_token = ?, lease_expiry = ?, attempt_count = attempt_count + 1, last_attempt_id = ? WHERE operation_id = ?",
        owner, OffsetDateTime.now().plus(Duration.ofMillis(leaseMs)), request.attemptId(), request.operationId());
  }

  private void persistEffect(InvocationRequest request, DownstreamResult result) throws JsonProcessingException {
    jdbc.update("UPDATE runtime.execution_ledger SET effect_reference = ?, result_payload = ?::jsonb WHERE operation_id = ?",
        result.effectId(), mapper.writeValueAsString(result.result()), request.operationId());
    transition(request, "EXECUTING", "EFFECT_CONFIRMED", "effect-confirmed");
  }

  private void persistEffectFromReconciling(InvocationRequest request, DownstreamResult result) throws JsonProcessingException {
    jdbc.update("UPDATE runtime.execution_ledger SET effect_reference = ?, result_payload = ?::jsonb, reconciliation_attempts = reconciliation_attempts + 1 WHERE operation_id = ?",
        result.effectId(), mapper.writeValueAsString(result.result()), request.operationId());
    transition(request, "RECONCILING", "EFFECT_CONFIRMED", "effect-confirmed-by-reconciliation");
  }

  private void transition(InvocationRequest request, String before, String after, String reason) {
    int updated = jdbc.update(
        "UPDATE runtime.execution_ledger SET state = ?::runtime.ledger_state WHERE operation_id = ? AND state = ?::runtime.ledger_state AND runtime.is_legal_ledger_transition(state, ?::runtime.ledger_state)",
        after, request.operationId(), before, after);
    if (updated != 1) throw new ResponseStatusException(HttpStatus.CONFLICT, "ILLEGAL_OR_STALE_LEDGER_TRANSITION");
    jdbc.update("INSERT INTO runtime.ledger_transitions(operation_id, state_before, state_after, attempt_id, reason) VALUES (?, ?::runtime.ledger_state, ?::runtime.ledger_state, ?, ?)",
        request.operationId(), before, after, request.attemptId(), reason);
  }

  private void markExpiredExecutingUnknown(InvocationRequest request) {
    jdbc.update("UPDATE runtime.execution_ledger SET state = 'UNKNOWN' WHERE operation_id = ? AND state = 'EXECUTING' AND lease_expiry < now()",
        request.operationId());
  }

  private JsonNode readTree(String json) {
    if (json == null) return null;
    try { return mapper.readTree(json); } catch (JsonProcessingException e) { throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "BAD_JSON", e); }
  }

  private void sleepForDelayedResponse() {
    try {
      Thread.sleep(10_000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "DELAY_INTERRUPTED", e);
    }
  }
}

class FaultDecider {
  boolean inject(InvocationRequest request, String hook) {
    if (request.failureScenario() == null || request.failureScenario().equals("F0")) return false;
    if (request.failureScenario().equals("F10")) {
      String mode = request.ledgerFailureMode() == null ? "read" : request.ledgerFailureMode();
      if (mode.equals("read") && !hook.equals("LEDGER_READ")) return false;
      if (mode.equals("write") && !hook.equals("LEDGER_WRITE")) return false;
    }
    if (!scenarioHook(request.failureScenario(), hook)) return false;
    if (request.failureProbability() >= 1.0) return true;
    if (request.failureProbability() <= 0.0) return false;
    String material = request.seed() + "|" + request.operationId() + "|" + request.failureScenario() + "|" + hook;
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(material.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      long value = ByteBuffer.wrap(digest, 0, 8).getLong() & Long.MAX_VALUE;
      double normalized = (double) value / (double) Long.MAX_VALUE;
      return normalized < request.failureProbability();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private boolean scenarioHook(String scenario, String hook) {
    return switch (scenario) {
      case "F1", "F2", "F7" -> hook.equals("BEFORE_DOWNSTREAM_DISPATCH");
      case "F3", "F8", "F12" -> hook.equals("BEFORE_EFFECT_CONFIRMATION_PERSIST");
      case "F4" -> hook.equals("AFTER_DOWNSTREAM_RESPONSE");
      case "F9" -> hook.equals("BEFORE_FINAL_RESULT_PERSIST");
      case "F10" -> hook.equals("LEDGER_READ") || hook.equals("LEDGER_WRITE");
      default -> false;
    };
  }
}
