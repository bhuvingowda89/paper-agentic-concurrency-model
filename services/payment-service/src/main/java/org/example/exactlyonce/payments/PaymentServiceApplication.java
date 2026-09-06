package org.example.exactlyonce.payments;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@SpringBootApplication
public class PaymentServiceApplication {
  public static void main(String[] args) {
    SpringApplication.run(PaymentServiceApplication.class, args);
  }
}

record DownstreamRequest(String operationId, String attemptId, String requestHash, String capability, String experimentId, String runId, JsonNode arguments) {}
record DownstreamResult(String effectId, String status, boolean replayed, JsonNode result) {}

@RestController
class PaymentController {
  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

  PaymentController(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @GetMapping("/health")
  Map<String, String> health() {
    return Map.of("status", "ok", "service", "payment-service");
  }

  @PostMapping("/execute")
  @Transactional
  DownstreamResult execute(@RequestBody DownstreamRequest request) throws JsonProcessingException {
    String capability = request.capability() == null ? "C0" : request.capability();
    if (!capability.equals("C0")) {
      DownstreamResult existing = lookupInternal(request.operationId());
      if (existing != null) {
        logCall(request, existing.effectId(), true);
        return new DownstreamResult(existing.effectId(), existing.status(), true, existing.result());
      }
    }
    JsonNode args = request.arguments();
    UUID paymentId = jdbc.queryForObject(
        "INSERT INTO runtime.payments(customer_id, amount) VALUES (?, ?) RETURNING payment_id",
        UUID.class,
        args.path("customer_id").asText(),
        new BigDecimal(args.path("amount").asText()));
    String effectId = paymentId.toString();
    JsonNode result = mapper.valueToTree(Map.of("payment_id", effectId, "status", "CHARGED"));
    if (!capability.equals("C0")) {
      jdbc.update("INSERT INTO runtime.service_idempotency(service_name, operation_id, request_hash, effect_id, result_payload) VALUES (?, ?, ?, ?, ?::jsonb)",
          "payment-service", request.operationId(), request.requestHash(), effectId, mapper.writeValueAsString(result));
    }
    jdbc.update("INSERT INTO observer.observer_effects(effect_id, operation_id, effect_type, service, experiment_id, run_id, payload) VALUES (?, ?, ?, ?, ?, ?, ?::jsonb)",
        effectId, request.operationId(), "CHARGE_PAYMENT", "payment-service", request.experimentId(), request.runId(), mapper.writeValueAsString(result));
    logCall(request, effectId, false);
    return new DownstreamResult(effectId, "OK", false, result);
  }

  private void logCall(DownstreamRequest request, String effectId, boolean replayed) {
    jdbc.update("INSERT INTO runtime.event_log(experiment_id, run_id, operation_id, attempt_id, service, event_type, tool_name, request_hash, downstream_effect_id, replayed, result_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        request.experimentId(), request.runId(), request.operationId(), request.attemptId(), "payment-service", "downstream_execute", "charge_payment", request.requestHash(), effectId, replayed, "OK");
  }

  @GetMapping("/lookup/{operationId}")
  DownstreamResult lookup(@PathVariable String operationId) {
    DownstreamResult result = lookupInternal(operationId);
    if (result == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "NO_EFFECT");
    return result;
  }

  private DownstreamResult lookupInternal(String operationId) {
    var rows = jdbc.query("SELECT effect_id, result_payload FROM runtime.service_idempotency WHERE service_name = ? AND operation_id = ?",
        (rs, rowNum) -> new DownstreamResult(rs.getString("effect_id"), "OK", true, readTree(rs.getString("result_payload"))),
        "payment-service", operationId);
    return rows.isEmpty() ? null : rows.getFirst();
  }

  private JsonNode readTree(String json) {
    try { return mapper.readTree(json); } catch (JsonProcessingException e) { throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "BAD_STORED_RESULT", e); }
  }
}
