import hashlib


HOOKS = [
    "BEFORE_GATEWAY_RECEIVE",
    "AFTER_GATEWAY_RECEIVE",
    "BEFORE_DOWNSTREAM_DISPATCH",
    "AFTER_DOWNSTREAM_DISPATCH",
    "AFTER_DOWNSTREAM_EFFECT_COMMIT",
    "BEFORE_DOWNSTREAM_RESPONSE",
    "AFTER_DOWNSTREAM_RESPONSE",
    "BEFORE_EFFECT_CONFIRMATION_PERSIST",
    "AFTER_EFFECT_CONFIRMATION_PERSIST",
    "BEFORE_FINAL_RESULT_PERSIST",
    "AFTER_FINAL_RESULT_PERSIST",
]


SCENARIO_HOOKS = {
    "F0": [],
    "F1": ["BEFORE_GATEWAY_RECEIVE"],
    "F2": ["BEFORE_DOWNSTREAM_DISPATCH"],
    "F3": ["BEFORE_EFFECT_CONFIRMATION_PERSIST"],
    "F4": ["AFTER_DOWNSTREAM_RESPONSE"],
    "F5": [],
    "F6": [],
    "F7": ["BEFORE_DOWNSTREAM_DISPATCH"],
    "F8": ["BEFORE_EFFECT_CONFIRMATION_PERSIST"],
    "F9": ["BEFORE_FINAL_RESULT_PERSIST"],
    "F10": ["LEDGER_READ", "LEDGER_WRITE"],
    "F11": ["RETRY_STORM"],
    "F12": ["BEFORE_EFFECT_CONFIRMATION_PERSIST"],
    "CF1": ["BEFORE_EFFECT_CONFIRMATION_PERSIST"],
    "CF2": ["BEFORE_EFFECT_CONFIRMATION_PERSIST", "CONCURRENT_RETRY"],
    "CF3": ["BEFORE_EFFECT_CONFIRMATION_PERSIST"],
    "CF4": ["LEDGER_READ", "LEDGER_WRITE", "RETRY_STORM"],
}


def selected(seed: int, operation_id: str, scenario: str, hook: str, probability: float) -> bool:
    if hook not in SCENARIO_HOOKS.get(scenario, []):
        return False
    if probability <= 0:
        return False
    if probability >= 1:
        return True
    material = f"{seed}|{operation_id}|{scenario}|{hook}".encode("utf-8")
    value = int.from_bytes(hashlib.sha256(material).digest()[:8], "big") / float(2**64 - 1)
    return value < probability
