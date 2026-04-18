"""
PlainSQL Load Test — Simulates real user behavior across all query types.

Run locally:
    locust -f tests/load/locustfile.py --host=http://localhost:8000

Web UI opens at http://localhost:8089 — configure users, ramp-up, and duration.

Recommended scenarios:
    Smoke:   5 users,  1/sec ramp,  2 min
    Normal: 30 users,  5/sec ramp, 10 min
    Peak:  100 users, 10/sec ramp, 10 min
    Soak:   30 users,  5/sec ramp, 60 min (memory leak detection)
"""

from locust import HttpUser, task, between, tag
import random

# ── Realistic query distribution ─────────────────────────
EASY_QUERIES = [
    "Show top 5 employees by salary",
    "List all products",
    "Show me customers from North America",
    "What is the most expensive product?",
    "Show me products with low stock",
]

MEDIUM_QUERIES = [
    "Total sales revenue by region",
    "Which department has the highest average salary?",
    "How many employees are in each department?",
    "Average product price by category",
]

HARD_QUERIES = [
    "Who are the top 3 salespeople by total revenue?",
    "Compare sales between Q1 and Q2",
    "Monthly sales trend for the last 6 months",
    "Which employees have sold more than the department average?",
]

CHAT_QUERIES = [
    "Hello",
    "What can you do?",
    "Thanks!",
    "Who are you?",
    "How does this work?",
]

# ── Repeat queries to test cache hit behavior ────────────
CACHED_QUERIES = [
    "Show top 5 employees by salary",
    "What is the total sales revenue?",
]


class PlainSQLUser(HttpUser):
    """Simulates a real user interacting with PlainSQL."""

    wait_time = between(2, 8)  # 2-8 seconds between requests (realistic pacing)

    def on_start(self):
        """Login and get JWT token on user spawn."""
        response = self.client.post(
            "/api/v1/auth/login",
            json={"username": "analyst", "password": "analyst123"},
            name="/auth/login",
        )
        if response.status_code == 200:
            self.token = response.json().get("access_token", "")
            self.headers = {"Authorization": f"Bearer {self.token}"}
        else:
            self.headers = {}

    def _chat(self, question: str, label: str):
        """Helper to send a chat request with auth headers."""
        self.client.post(
            "/chat",
            json={"question": question, "history": []},
            headers=self.headers,
            name=f"/chat [{label}]",
        )

    @task(40)
    @tag("easy")
    def easy_query(self):
        """Simple data queries — should complete in 1-3s."""
        self._chat(random.choice(EASY_QUERIES), "easy")

    @task(20)
    @tag("medium")
    def medium_query(self):
        """Aggregation queries — medium complexity."""
        self._chat(random.choice(MEDIUM_QUERIES), "medium")

    @task(10)
    @tag("hard")
    def hard_query(self):
        """Complex JOIN queries — expected 2-4s."""
        self._chat(random.choice(HARD_QUERIES), "hard")

    @task(10)
    @tag("chat")
    def chat_message(self):
        """Conversational messages — should return in <100ms (no LLM)."""
        self._chat(random.choice(CHAT_QUERIES), "chat")

    @task(15)
    @tag("cached")
    def cached_query(self):
        """Repeat queries — should be served from cache in <10ms."""
        self._chat(random.choice(CACHED_QUERIES), "cached")

    @task(5)
    @tag("health")
    def health_check(self):
        """Health endpoint — should always respond in <50ms."""
        self.client.get("/api/v1/health", name="/health")
