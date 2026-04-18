"""
Input Validator — Detects prompt injection and sanitizes user input.
Runs before user queries reach the LLM pipeline, providing defense-in-depth.
"""

import re
import structlog
from typing import Optional

logger = structlog.get_logger()

# ── Prompt Injection Patterns ────────────────────────────
# These patterns detect common prompt injection techniques where
# attackers try to override the system prompt or extract instructions.

_INJECTION_PATTERNS: list[tuple[str, str]] = [
    # Direct instruction override
    (r"ignore\s+(all\s+)?(previous|above|prior)\s+(instructions?|prompts?|rules?)", "instruction_override"),
    (r"disregard\s+(all\s+)?(previous|above|prior)", "instruction_override"),
    (r"forget\s+(everything|all|your)\s+(instructions?|rules?|training)", "instruction_override"),
    (r"override\s+(the\s+)?(system|safety|security)", "instruction_override"),
    (r"new\s+instructions?\s*:", "instruction_override"),

    # Role reassignment
    (r"you\s+are\s+now\s+(?:a|an)\s+", "role_reassignment"),
    (r"act\s+as\s+(?:a|an)\s+(?!data|sql|query)", "role_reassignment"),
    (r"pretend\s+(?:you(?:'re|\s+are)\s+|to\s+be\s+)", "role_reassignment"),
    (r"switch\s+to\s+.+\s+mode", "role_reassignment"),

    # System prompt extraction
    (r"(show|reveal|display|print|output|repeat)\s+(your\s+)?(system\s+)?(prompt|instructions?|rules?)", "prompt_extraction"),
    (r"what\s+(?:are|is)\s+your\s+(system\s+)?(prompt|instructions?|rules?)", "prompt_extraction"),

    # Jailbreak markers
    (r"\bDAN\b", "jailbreak_marker"),
    (r"developer\s+mode", "jailbreak_marker"),
    (r"do\s+anything\s+now", "jailbreak_marker"),
    (r"jailbreak", "jailbreak_marker"),

    # SQL smuggling via natural language
    (r";\s*(DROP|DELETE|UPDATE|INSERT|ALTER|TRUNCATE|GRANT|EXEC)", "sql_smuggling"),
    (r"UNION\s+(ALL\s+)?SELECT\s+", "sql_smuggling"),

    # Prompt delimiter injection (trying to close the system prompt)
    (r"```\s*\n\s*(system|assistant|user)\s*:", "delimiter_injection"),
    (r"<\|im_start\|>", "delimiter_injection"),
    (r"<\|endoftext\|>", "delimiter_injection"),
    (r"\[INST\]", "delimiter_injection"),
    (r"<<SYS>>", "delimiter_injection"),
]

# Pre-compile for performance
_COMPILED_PATTERNS = [
    (re.compile(pattern, re.IGNORECASE), label)
    for pattern, label in _INJECTION_PATTERNS
]


class InputValidator:
    """
    Validates and sanitizes user input before it enters the LLM pipeline.
    
    Defense layers:
    1. Length and content validation
    2. Prompt injection pattern detection
    3. Character-set sanitization
    4. Conversation history sanitization
    """

    def __init__(self, max_length: int = 1000, strict_mode: bool = False):
        """
        Args:
            max_length: Maximum allowed query length.
            strict_mode: If True, block on any detection. If False, log warnings
                         but only block high-confidence attacks.
        """
        self.max_length = max_length
        self.strict_mode = strict_mode

    def validate(self, query: str) -> tuple[bool, Optional[str], str]:
        """
        Validate a user query.
        
        Returns:
            (is_safe, rejection_reason, sanitized_query)
            - is_safe: True if the query passes all checks
            - rejection_reason: Human-readable reason if blocked, None if safe
            - sanitized_query: Cleaned version of the input
        """
        if not query or not query.strip():
            return False, "Empty query", ""

        # ── 1. Length check ──────────────────────────────
        if len(query) > self.max_length:
            return False, f"Query too long ({len(query)} chars, max {self.max_length})", ""

        # ── 2. Sanitize control characters ───────────────
        sanitized = self._sanitize(query)

        # ── 3. Prompt injection detection ────────────────
        detections = self._detect_injections(sanitized)

        if detections:
            labels = [d[1] for d in detections]
            logger.warning(
                "prompt_injection_detected",
                query_preview=sanitized[:80],
                patterns=labels,
            )

            # High-severity patterns always block
            high_severity = {"instruction_override", "jailbreak_marker", "sql_smuggling", "delimiter_injection"}
            if high_severity.intersection(labels) or self.strict_mode:
                return False, f"Query blocked: suspicious pattern detected ({', '.join(labels)})", sanitized

            # Medium severity: log but allow (the SQL validator will catch actual attacks)

        return True, None, sanitized

    def sanitize_history(self, history: list[dict]) -> list[dict]:
        """
        Sanitize conversation history entries before injecting into LLM prompts.
        Strips any content that looks like prompt injection from stored messages.
        
        This prevents a stored XSS-style attack where a malicious user message
        is saved to the DB and later injected into the prompt template for
        the NEXT query's context window.
        """
        safe_history = []
        for entry in (history or []):
            safe_entry = {}
            for key, value in entry.items():
                if isinstance(value, str):
                    # Strip control characters
                    cleaned = self._sanitize(value)
                    # Remove any high-severity injection patterns from history
                    detections = self._detect_injections(cleaned)
                    high_severity = {"instruction_override", "jailbreak_marker", "delimiter_injection"}
                    labels = {d[1] for d in detections}
                    if high_severity.intersection(labels):
                        logger.warning("history_injection_stripped", key=key, patterns=list(labels))
                        cleaned = "[content filtered for safety]"
                    safe_entry[key] = cleaned
                else:
                    safe_entry[key] = value
            safe_history.append(safe_entry)
        return safe_history

    def _sanitize(self, query: str) -> str:
        """Remove control characters and normalize whitespace."""
        # Remove null bytes and control chars (keep newlines, tabs)
        cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', query)
        # Remove Unicode zero-width characters (used for obfuscation)
        cleaned = re.sub(r'[\u200b\u200c\u200d\u200e\u200f\ufeff]', '', cleaned)
        # Normalize whitespace
        cleaned = ' '.join(cleaned.split())
        return cleaned.strip()

    def _detect_injections(self, query: str) -> list[tuple[str, str]]:
        """Run all injection detection patterns against the query."""
        detections = []
        for pattern, label in _COMPILED_PATTERNS:
            if pattern.search(query):
                detections.append((pattern.pattern, label))
        return detections


# ── Module-level singleton ───────────────────────────────
_default_validator = InputValidator()


def validate_query(query: str) -> tuple[bool, Optional[str], str]:
    """Convenience function using the default validator."""
    return _default_validator.validate(query)

