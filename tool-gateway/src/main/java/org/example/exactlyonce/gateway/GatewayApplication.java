package org.example.exactlyonce.gateway;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.HexFormat;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.server.ResponseStatusException;

@SpringBootApplication
public class GatewayApplication {
  public static void main(String[] args) {
    SpringApplication.run(GatewayApplication.class, args);
  }
}

record ToolRequest(
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

@RestController
class GatewayController {
  private final RestTemplate http = new RestTemplate();
  private final ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  private final String orchestratorUrl;

  GatewayController(@Value("${orchestrator.url}") String orchestratorUrl) {
    this.orchestratorUrl = orchestratorUrl;
  }

  @GetMapping("/health")
  Map<String, String> health() {
    return Map.of("status", "ok", "service", "tool-gateway");
  }

  @PostMapping("/tools/{toolName}")
  ResponseEntity<Object> invoke(@PathVariable String toolName, @RequestBody ToolRequest request) {
    if (!toolName.equals(request.toolName())) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "TOOL_NAME_MISMATCH");
    String computed = requestHash(toolName, request.arguments());
    if (!computed.equals(request.requestHash())) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "REQUEST_HASH_MISMATCH");
    try {
      Object body = http.postForObject(orchestratorUrl + "/invoke", request, Object.class);
      return ResponseEntity.ok(body);
    } catch (HttpStatusCodeException e) {
      return ResponseEntity.status(e.getStatusCode()).body(e.getResponseBodyAsString());
    }
  }

  private String requestHash(String toolName, JsonNode arguments) {
    try {
      String canonicalArguments = canonicalize(arguments);
      byte[] digest = MessageDigest.getInstance("SHA-256").digest((toolName + canonicalArguments).getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest);
    } catch (Exception e) {
      throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "HASH_FAILURE", e);
    }
  }

  private String canonicalize(JsonNode node) throws JsonProcessingException {
    if (node == null || node.isNull()) return "null";
    if (node.isObject()) {
      Map<String, Object> ordered = new TreeMap<>();
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        ordered.put(field.getKey(), canonicalValue(field.getValue()));
      }
      return mapper.writeValueAsString(ordered);
    }
    return mapper.writeValueAsString(canonicalValue(node));
  }

  private Object canonicalValue(JsonNode node) {
    if (node == null || node.isNull()) return null;
    if (node.isObject()) {
      Map<String, Object> ordered = new TreeMap<>();
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        ordered.put(field.getKey(), canonicalValue(field.getValue()));
      }
      return ordered;
    }
    if (node.isArray()) {
      List<Object> items = new ArrayList<>();
      for (JsonNode item : node) {
        items.add(canonicalValue(item));
      }
      return items;
    }
    if (node.isTextual()) return node.asText();
    if (node.isIntegralNumber()) return node.longValue();
    if (node.isFloatingPointNumber() || node.isBigDecimal()) return node.decimalValue();
    if (node.isBoolean()) return node.booleanValue();
    return node.asText();
  }
}
