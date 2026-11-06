"""Incremental, fail-closed redaction for a documented subset of PII patterns."""

from __future__ import annotations

import re

MARKER = "[REDACTED]"
MAX_PENDING = 256
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
EMAIL_PARTIAL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]*")
SSN = re.compile(r"(?<!\d)\d{3}-\d{2}-\d{4}(?!\d)")
SSN_PARTIAL = re.compile(r"(?<!\d)\d{3}-\d{0,2}(?:-\d{0,3})?(?!\d)")
NUMBER_RUN = re.compile(r"\d(?:[\d -]*\d)?")
POTENTIAL_SUFFIX = re.compile(r"(?:[A-Za-z0-9._%+@-]+|\d[\d -]*)$")
EMAIL_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._%+@-")
NUMBER_CHARS = frozenset("0123456789 -")


def _luhn(digits: str) -> bool:
    total = 0
    for index, character in enumerate(reversed(digits)):
        value = int(character)
        if index % 2:
            value *= 2
            if value > 9:
                value -= 9
        total += value
    return total % 10 == 0


def redact_complete(text: str) -> str:
    """Redact complete candidates only; pending suffixes are held by Redactor."""
    text = EMAIL.sub(MARKER, text)
    text = EMAIL_PARTIAL.sub(MARKER, text)
    text = SSN.sub(MARKER, text)
    text = SSN_PARTIAL.sub(MARKER, text)

    def card(match: re.Match[str]) -> str:
        candidate = match.group()
        digits = re.sub(r"\D", "", candidate)
        if 13 <= len(digits) <= 19 and _luhn(digits):
            return MARKER
        # A grouped 12-digit run can be an unfinished card candidate.
        if len(digits) == 12 and any(separator in candidate for separator in " -"):
            return MARKER
        return candidate

    return NUMBER_RUN.sub(card, text)


class Redactor:
    """Hold unresolved email/number suffixes; redact overlong candidates once."""

    def __init__(self) -> None:
        self.pending = ""
        self.suppress: frozenset[str] | None = None
        self.max_pending_seen = 0
        self.chunks = 0

    def feed(self, delta: str) -> str:
        self.chunks += 1
        output: list[str] = []
        for character in delta:
            if self.suppress is not None:
                if character in self.suppress:
                    continue
                self.suppress = None
            if character == "." and re.fullmatch(r"\d[\d -]*", self.pending) and redact_complete(self.pending) == MARKER:
                # A card/SSN can end immediately before a period. Commit the
                # redaction before the generic suffix matcher sees trailing digits.
                output.append(MARKER)
                self.pending = ""
            self.pending += character
            match = POTENTIAL_SUFFIX.search(self.pending)
            start = match.start() if match else len(self.pending)
            if start:
                output.append(redact_complete(self.pending[:start]))
                self.pending = self.pending[start:]
            if len(self.pending) > MAX_PENDING:
                # Never emit an unresolved candidate merely to keep the buffer small.
                self.suppress = NUMBER_CHARS if re.fullmatch(r"\d[\d -]*", self.pending) else EMAIL_CHARS
                self.pending = ""
                output.append(MARKER)
            self.max_pending_seen = max(self.max_pending_seen, len(self.pending))
        return "".join(output)

    def finish(self) -> str:
        if self.suppress is not None:
            self.suppress = None
            return ""
        result = redact_complete(self.pending)
        self.pending = ""
        return result
