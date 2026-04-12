const API = {
  chat: "/chat",
  health: "/api/v1/health",
  schema: "/api/v1/schema"
};

const dom = {
  sidebar: document.getElementById("sidebar"),
  chatRoot: document.getElementById("chatRoot"),
  chatScroll: document.getElementById("chatScroll"),
  form: document.getElementById("composerForm"),
  input: document.getElementById("promptInput"),
  send: document.getElementById("sendButton"),
  suggestions: document.getElementById("suggestions"),
  title: document.getElementById("conversationTitle"),
  subtitle: document.getElementById("conversationSubtitle"),
  healthChip: document.getElementById("healthChip"),
  toasts: document.getElementById("toasts")
};

const state = {
  chats: [],
  activeChatId: null,
  selectedSchema: "default",
  schemaTables: ["default"],
  savedQueries: [
    "Show top 5 employees by salary",
    "Total sales revenue by region",
    "Which department has the highest spend?",
    "Show products with low stock"
  ],
  system: {
    status: "Checking",
    latency: null,
    schemaSource: "Loading"
  },
  isSending: false,
  charts: new Map()
};

const Component = {
  Sidebar() {
    const activeChat = getActiveChat();
    const chats = state.chats.length ? state.chats : [{ id: "empty", title: "No conversations yet", messages: [] }];
    const metrics = getWorkspaceMetrics();
    return `
      <div class="sidebar-head">
        <div class="brand">
          <div class="brand-mark">SQL</div>
          <div class="brand-copy"><strong>PlainSQL</strong><span>AI data workspace</span></div>
        </div>
        <button class="new-chat" type="button" data-action="new-chat">New chat</button>
      </div>
      <div class="sidebar-scroll">
        <div class="section">
          <div class="section-title"><span>Database</span><span>${state.schemaTables.length} tables</span></div>
          <div class="schema-panel">
            <label class="field-label" for="schemaSelect">Schema selector</label>
            <select class="select" id="schemaSelect" data-action="select-schema">
              ${state.schemaTables.map(name => `<option value="${escapeAttr(name)}" ${name === state.selectedSchema ? "selected" : ""}>${escapeHtml(name)}</option>`).join("")}
            </select>
            <div class="schema-meta">
              <span>${state.selectedSchema === "default" ? "Using default context" : "Table context selected"}</span>
              <span>Read-only</span>
            </div>
          </div>
        </div>
        <div class="section">
          <div class="section-title"><span>Workspace</span><span>Live</span></div>
          <div class="workspace-card">
            <div class="metric-grid">
              <div><strong>${metrics.questions}</strong><span>Questions</span></div>
              <div><strong>${metrics.sqlBlocks}</strong><span>SQL blocks</span></div>
              <div><strong>${metrics.resultRows}</strong><span>Rows seen</span></div>
              <div><strong>${state.system.latency ?? "--"}</strong><span>API ms</span></div>
            </div>
            <div class="pipeline">
              ${["Understand", "Generate", "Validate", "Visualize"].map((step, index) => `<span class="${index <= metrics.pipelineIndex ? "active" : ""}">${step}</span>`).join("")}
            </div>
          </div>
        </div>
        <div class="section">
          <div class="section-title"><span>Chat history</span><span>${state.chats.length}</span></div>
          <div class="history-list">
            ${chats.map(chat => `
              <button class="side-item ${activeChat && chat.id === activeChat.id ? "active" : ""}" type="button" data-chat-id="${escapeAttr(chat.id)}" ${chat.id === "empty" ? "disabled" : ""}>
                <span>${escapeHtml(chat.title)}</span>${chat.messages.length ? `<small>${chat.messages.length}</small>` : ""}
              </button>
            `).join("")}
          </div>
        </div>
        <div class="section">
          <div class="section-title"><span>Saved queries</span><span>${state.savedQueries.length}</span></div>
          <div class="saved-list">
            ${state.savedQueries.map(query => `
              <button class="side-item" type="button" data-saved-query="${escapeAttr(query)}"><span>${escapeHtml(query)}</span></button>
            `).join("")}
          </div>
        </div>
      </div>
      <div class="sidebar-foot"><div class="system-pill ${state.system.status.toLowerCase().includes("offline") ? "warning" : ""}"><span>${escapeHtml(state.system.status)}</span><span class="status-dot"></span></div></div>
    `;
  },

  ChatWindow() {
    const chat = getActiveChat();
    if (!chat || chat.messages.length === 0) return Component.Welcome();
    return chat.messages.map(message => Component.MessageBubble(message)).join("");
  },

  Welcome() {
    const prompts = [
      ["Revenue pulse", "Total sales revenue by region"],
      ["Customer concentration", "Which customers generated the most revenue?"],
      ["Inventory watch", "Show products with low stock"],
      ["Team insights", "Show top 5 employees by salary"]
    ];
    return `
      <div class="welcome">
        <div class="welcome-badge">Production text-to-SQL workspace</div>
        <h2>Turn database questions into <span>reviewable decisions.</span></h2>
        <p>Ask in plain English, inspect the SQL, scan the answer, and keep the next question moving.</p>
        <div class="command-strip" aria-label="Workflow">
          <span>Natural language</span>
          <span>Safe SQL</span>
          <span>Results</span>
          <span>Insights</span>
        </div>
        <div class="prompt-grid">
          ${prompts.map(([title, query]) => `
            <button class="prompt-card" type="button" data-saved-query="${escapeAttr(query)}">
              <strong>${escapeHtml(title)}</strong><span>${escapeHtml(query)}</span>
            </button>
          `).join("")}
        </div>
      </div>
    `;
  },

  MessageBubble(message) {
    const role = message.role === "user" ? "user" : "ai";
    const body = message.pending
      ? Component.LoadingState()
      : role === "user"
        ? `<div>${escapeHtml(message.content)}</div>`
        : Component.AssistantContent(message);
    return `
      <article class="message ${role}" data-message-id="${escapeAttr(message.id)}">
        ${role === "user" ? `<div class="bubble">${body}</div><div class="avatar">YOU</div>` : `<div class="avatar ai">AI</div><div class="bubble">${body}</div>`}
      </article>
    `;
  },

  LoadingState() {
    return `<div class="loading-row" aria-label="Loading response"><div class="loading-copy">Coordinating schema context, SQL safety, and result rendering</div><div class="skeleton"></div><div class="skeleton"></div><div class="skeleton"></div></div>`;
  },

  AssistantContent(message) {
    if (message.error) return `<div class="error-box">${escapeHtml(message.error)}</div>`;
    const data = message.data || {};
    const rows = normalizeRows(data);
    const stream = message.streamText || data.message || "I generated the SQL and result set below.";
    return `
      <div class="bubble-stream ${message.streaming ? "typing-caret" : ""}">${escapeHtml(stream)}</div>
      ${Component.MetaRow(data, rows)}
      ${rows.length ? Component.ResultSummary(rows) : ""}
      ${data.sql ? Component.SQLBlock(data.sql, message.id) : ""}
      ${rows.length ? Component.ResultTable(rows, message.id) : data.sql ? Component.EmptyState() : ""}
      ${rows.length && hasChartSupport(rows) ? Component.ChartView(message.id) : ""}
      ${data.explanation || data.sql_explanation ? Component.ExplanationBlock(data.explanation || data.sql_explanation) : ""}
      ${Array.isArray(data.insights) && data.insights.length ? Component.InsightBlock(data.insights) : ""}
    `;
  },

  MetaRow(data, rows) {
    const items = [];
    if (data.intent) items.push(["Intent", data.intent]);
    if (typeof data.execution_time_ms !== "undefined") items.push(["Latency", `${Math.round(Number(data.execution_time_ms) || 0)}ms`]);
    if (typeof data.row_count !== "undefined" || rows.length) items.push(["Rows", data.row_count ?? rows.length]);
    if (data.trace_id) items.push(["Trace", data.trace_id]);
    return items.length ? `<div class="meta-row">${items.map(([label, value]) => `<span class="meta">${escapeHtml(label)} <b>${escapeHtml(String(value))}</b></span>`).join("")}</div>` : "";
  },

  SQLBlock(sql, messageId) {
    return `
      <section class="sql-block">
        <div class="block-head">
          <span class="block-title">Generated SQL</span>
          <div class="block-actions">
            <button class="small-button" type="button" data-copy-sql="${escapeAttr(messageId)}">Copy SQL</button>
            <button class="small-button" type="button" data-save-sql="${escapeAttr(messageId)}">Save query</button>
          </div>
        </div>
        <pre><code>${highlightSQL(sql)}</code></pre>
      </section>
    `;
  },

  ResultSummary(rows) {
    const summary = summarizeRows(rows);
    return `
      <section class="summary-strip" aria-label="Result summary">
        ${summary.map(item => `<div><span>${escapeHtml(item.label)}</span><strong>${escapeHtml(item.value)}</strong></div>`).join("")}
      </section>
    `;
  },

  ResultTable(rows, messageId) {
    const cols = Object.keys(rows[0] || {});
    return `
      <section class="result-block">
        <div class="block-head">
          <span class="block-title">Result table</span>
          <div class="block-actions">
            <button class="small-button" type="button" data-export-csv="${escapeAttr(messageId)}">Export CSV</button>
            <button class="small-button" type="button" data-copy-result="${escapeAttr(messageId)}">Copy JSON</button>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr>${cols.map(col => `<th>${escapeHtml(col)}</th>`).join("")}</tr></thead>
            <tbody>${rows.slice(0, 100).map(row => `<tr>${cols.map(col => `<td>${escapeHtml(formatCell(row[col]))}</td>`).join("")}</tr>`).join("")}</tbody>
          </table>
        </div>
      </section>
    `;
  },

  ChartView(messageId) {
    return `
      <section class="chart-block">
        <div class="block-head">
          <span class="block-title">Chart view</span>
          <div class="block-actions">
            <button class="small-button chart-toggle" type="button" data-chart-type="${escapeAttr(messageId)}" data-type="bar">Bar</button>
            <button class="small-button chart-toggle" type="button" data-chart-type="${escapeAttr(messageId)}" data-type="line">Line</button>
          </div>
        </div>
        <div class="chart-body"><canvas id="chart-${escapeAttr(messageId)}"></canvas></div>
      </section>
    `;
  },

  ExplanationBlock(explanation) {
    return `<section class="explanation-block"><h3 class="insight-title">SQL reasoning</h3><p>${escapeHtml(stripMarkdown(String(explanation)))}</p></section>`;
  },

  InsightBlock(insights) {
    return `<section class="insight-block"><h3 class="insight-title">AI insights</h3><ul class="insight-list">${insights.map(item => `<li>${escapeHtml(stripMarkdown(String(item)))}</li>`).join("")}</ul></section>`;
  },

  EmptyState() {
    return `<div class="error-box empty-result">No rows matched this query.</div>`;
  },

  Suggestions(items) {
    return items.map(item => `<button class="suggestion" type="button" data-saved-query="${escapeAttr(item)}">${escapeHtml(item)}</button>`).join("");
  }
};

function render() {
  dom.sidebar.innerHTML = Component.Sidebar();
  dom.chatRoot.innerHTML = Component.ChatWindow();
  const chat = getActiveChat();
  dom.title.textContent = chat?.title || "Data analysis chat";
  dom.subtitle.textContent = chat?.messages.length ? `${chat.messages.length} messages in this workspace` : "Ask in natural language. Review the SQL. Trust the result after inspection.";
  hydrateCharts();
  scrollToBottom();
}

function getActiveChat() {
  return state.chats.find(chat => chat.id === state.activeChatId) || null;
}

function ensureChat(seedTitle = "Untitled analysis") {
  let chat = getActiveChat();
  if (chat) return chat;
  chat = { id: createId("chat"), title: seedTitle, messages: [], context: [] };
  state.chats.unshift(chat);
  state.activeChatId = chat.id;
  return chat;
}

function newChat() {
  const chat = { id: createId("chat"), title: "New analysis", messages: [], context: [] };
  state.chats.unshift(chat);
  state.activeChatId = chat.id;
  state.charts.forEach(chart => chart.destroy());
  state.charts.clear();
  dom.suggestions.innerHTML = "";
  render();
  dom.input.focus();
}

async function submitPrompt(raw) {
  const question = String(raw ?? "").trim();
  if (!question || state.isSending) return;
  const chat = ensureChat(titleFromPrompt(question));
  if (!chat.messages.length || chat.title === "New analysis" || chat.title === "Untitled analysis") chat.title = titleFromPrompt(question);
  const pending = { id: createId("msg"), role: "assistant", pending: true, data: null };
  chat.messages.push({ id: createId("msg"), role: "user", content: question }, pending);
  dom.input.value = "";
  autoSizeInput();
  dom.suggestions.innerHTML = "";
  setSending(true);
  render();

  try {
    const payload = {
      question: state.selectedSchema === "default" ? question : `${question}\n\nUse table context: ${state.selectedSchema}`,
      history: chat.context.slice(-6)
    };
    const response = await fetch(API.chat, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.error) throw new Error(data.error || `Request failed with ${response.status}`);

    pending.pending = false;
    pending.data = data;
    pending.streamText = "";
    pending.streaming = true;
    render();
    await streamResponse(pending, data.message || "Done. I generated a read-only SQL query and prepared the result view.");
    pending.streaming = false;
    if (data.sql && !String(data.sql).toLowerCase().includes("error")) {
      chat.context.push({ user: question, sql: data.sql });
      chat.context = chat.context.slice(-8);
    }
    dom.suggestions.innerHTML = Component.Suggestions(Array.isArray(data.follow_ups) ? data.follow_ups.slice(0, 5) : []);
    toast("Query complete", "success");
  } catch (error) {
    pending.pending = false;
    pending.error = `${error.message}. Make sure the FastAPI backend is running and reachable.`;
    render();
    toast("Query failed", "error");
  } finally {
    setSending(false);
    render();
  }
}

async function streamResponse(message, text) {
  const tokens = String(text).split(/(\s+)/).filter(Boolean);
  for (const token of tokens) {
    message.streamText += token;
    updateMessage(message);
    await wait(token.trim() ? 24 : 6);
  }
}

function updateMessage(message) {
  const node = document.querySelector(`[data-message-id="${escapeSelector(message.id)}"] .bubble`);
  if (!node) return;
  node.innerHTML = Component.AssistantContent(message);
  hydrateCharts();
  scrollToBottom();
}

function hydrateCharts(typeOverrideById = {}) {
  const chat = getActiveChat();
  if (!chat) return;
  chat.messages.filter(message => message.role === "assistant" && message.data && hasChartSupport(normalizeRows(message.data))).forEach(message => {
    const canvas = document.getElementById(`chart-${message.id}`);
    if (!canvas || !window.Chart) return;
    const rows = normalizeRows(message.data);
    const type = typeOverrideById[message.id] || canvas.dataset.type || inferChartType(rows);
    canvas.dataset.type = type;
    document.querySelectorAll(`[data-chart-type="${escapeSelector(message.id)}"]`).forEach(button => {
      button.classList.toggle("active", button.dataset.type === type);
    });
    if (state.charts.has(message.id)) state.charts.get(message.id).destroy();
    state.charts.set(message.id, new Chart(canvas.getContext("2d"), buildChartConfig(rows, type)));
  });
}

function buildChartConfig(rows, type) {
  const cols = getColumns(rows);
  const numericCol = getNumericColumns(rows)[0] || cols[0];
  const labelCol = cols.find(col => col !== numericCol && rows.some(row => !isNumericValue(row[col]))) || cols.find(col => col !== numericCol) || cols[0];
  return {
    type,
    data: {
      labels: rows.slice(0, 24).map(row => String(row[labelCol] ?? "").slice(0, 28)),
      datasets: [{
        label: readableLabel(numericCol),
        data: rows.slice(0, 24).map(row => toNumber(row[numericCol])),
        borderColor: "#2fd6c2",
        backgroundColor: type === "line" ? "rgba(47,214,194,0.14)" : "rgba(47,214,194,0.68)",
        pointBackgroundColor: "#f0c85a",
        pointRadius: type === "line" ? 3 : 0,
        borderWidth: 2,
        borderRadius: type === "bar" ? 6 : 0,
        tension: 0.36,
        fill: type === "line"
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: "#b9c1bd", font: { family: "Inter" } } },
        tooltip: { backgroundColor: "#111715", borderColor: "rgba(238,243,239,0.2)", borderWidth: 1 }
      },
      scales: {
        x: { ticks: { color: "#7f8a85" }, grid: { color: "rgba(238,243,239,0.06)" } },
        y: { ticks: { color: "#7f8a85" }, grid: { color: "rgba(238,243,239,0.08)" } }
      }
    }
  };
}

function inferChartType(rows) {
  const cols = getColumns(rows);
  return cols.some(col => /date|month|year|day|time/i.test(col)) ? "line" : "bar";
}

async function loadHealth() {
  try {
    const started = performance.now();
    const response = await fetch(API.health);
    const health = await response.json();
    state.system.latency = Math.max(1, Math.round(performance.now() - started));
    state.system.status = health.status === "healthy" ? "Healthy" : "Degraded";
    dom.healthChip.textContent = state.system.status;
  } catch {
    state.system.status = "Offline mode";
    state.system.latency = null;
    dom.healthChip.textContent = state.system.status;
  }
  render();
}

async function loadSchema() {
  try {
    const token = localStorage.getItem("plainsql_token");
    const headers = token ? { Authorization: `Bearer ${token}` } : {};
    const response = await fetch(API.schema, { headers });
    if (!response.ok) throw new Error("Schema requires auth");
    const schema = await response.json();
    state.schemaTables = ["default", ...(schema.tables || [])];
    state.system.schemaSource = "Connected";
  } catch {
    state.schemaTables = ["default", "employees", "sales", "products", "customers"];
    state.system.schemaSource = "Demo fallback";
  }
  render();
}

function handleClick(event) {
  const openSidebar = event.target.closest("[data-action='open-sidebar']");
  const closeSidebar = event.target.closest("[data-action='close-sidebar']");
  const newChatButton = event.target.closest("[data-action='new-chat']");
  const historyButton = event.target.closest("[data-chat-id]");
  const savedButton = event.target.closest("[data-saved-query]");
  const copySql = event.target.closest("[data-copy-sql]");
  const saveSql = event.target.closest("[data-save-sql]");
  const exportCsv = event.target.closest("[data-export-csv]");
  const copyResult = event.target.closest("[data-copy-result]");
  const chartType = event.target.closest("[data-chart-type]");
  if (openSidebar) document.body.classList.add("sidebar-open");
  if (closeSidebar) document.body.classList.remove("sidebar-open");
  if (newChatButton) newChat();
  if (historyButton && historyButton.dataset.chatId !== "empty") {
    state.activeChatId = historyButton.dataset.chatId;
    document.body.classList.remove("sidebar-open");
    render();
  }
  if (savedButton) {
    document.body.classList.remove("sidebar-open");
    submitPrompt(savedButton.dataset.savedQuery);
  }
  if (copySql) writeClipboard(findMessage(copySql.dataset.copySql)?.data?.sql || "", "SQL copied");
  if (saveSql) {
    const sql = findMessage(saveSql.dataset.saveSql)?.data?.sql || "";
    if (sql && !state.savedQueries.includes(sql)) state.savedQueries.unshift(sql);
    toast("Saved query added", "success");
    render();
  }
  if (exportCsv) downloadCSV(normalizeRows(findMessage(exportCsv.dataset.exportCsv)?.data || {}));
  if (copyResult) writeClipboard(JSON.stringify(normalizeRows(findMessage(copyResult.dataset.copyResult)?.data || {}), null, 2), "Result JSON copied");
  if (chartType) {
    const id = chartType.dataset.chartType;
    const canvas = document.getElementById(`chart-${id}`);
    if (canvas) canvas.dataset.type = chartType.dataset.type;
    hydrateCharts({ [id]: chartType.dataset.type });
  }
}

function handleChange(event) {
  if (event.target.matches("[data-action='select-schema']")) {
    state.selectedSchema = event.target.value;
    toast(`Schema context: ${state.selectedSchema}`, "success");
    render();
  }
}

function handleSubmit(event) {
  event.preventDefault();
  submitPrompt(dom.input.value);
}

function handleInputKeydown(event) {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    submitPrompt(dom.input.value);
  }
}

function autoSizeInput() {
  dom.input.style.height = "auto";
  dom.input.style.height = `${Math.min(dom.input.scrollHeight, 150)}px`;
}

function setSending(value) {
  state.isSending = value;
  dom.send.disabled = value;
}

function getWorkspaceMetrics() {
  const messages = state.chats.flatMap(chat => chat.messages);
  const assistantMessages = messages.filter(message => message.role === "assistant" && message.data);
  const resultRows = assistantMessages.reduce((total, message) => total + normalizeRows(message.data).length, 0);
  const sqlBlocks = assistantMessages.filter(message => message.data?.sql).length;
  return {
    questions: messages.filter(message => message.role === "user").length,
    sqlBlocks,
    resultRows,
    pipelineIndex: state.isSending ? 1 : sqlBlocks ? 3 : 0
  };
}

function normalizeRows(data) {
  const rows = data?.answer || data?.data || [];
  return Array.isArray(rows) && rows.length && typeof rows[0] === "object" ? rows : [];
}

function summarizeRows(rows) {
  const cols = getColumns(rows);
  const numericCols = getNumericColumns(rows);
  const labelCol = cols.find(col => !numericCols.includes(col)) || cols[0] || "Column";
  const primaryMetric = numericCols[0];
  const total = primaryMetric
    ? rows.reduce((sum, row) => sum + toNumber(row[primaryMetric]), 0)
    : rows.length;
  const peak = primaryMetric
    ? rows.reduce((best, row) => toNumber(row[primaryMetric]) > toNumber(best?.[primaryMetric]) ? row : best, rows[0])
    : rows[0];
  return [
    { label: "Rows", value: formatNumber(rows.length) },
    { label: primaryMetric ? `Total ${readableLabel(primaryMetric)}` : "Records", value: primaryMetric ? formatNumber(total) : formatNumber(rows.length) },
    { label: "Primary label", value: readableLabel(labelCol) },
    { label: "Top result", value: formatCell(peak?.[labelCol] ?? "Ready") || "Ready" }
  ];
}

function formatNumber(value) {
  return Number(value || 0).toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function hasChartSupport(rows) {
  return Boolean(window.Chart && rows.length && getNumericColumns(rows).length);
}

function getColumns(rows) {
  return Array.from(rows.reduce((set, row) => {
    Object.keys(row || {}).forEach(key => set.add(key));
    return set;
  }, new Set()));
}

function getNumericColumns(rows) {
  return getColumns(rows).filter(col => rows.some(row => isNumericValue(row[col])));
}

function isNumericValue(value) {
  if (typeof value === "number") return Number.isFinite(value);
  if (typeof value !== "string") return false;
  const trimmed = value.trim();
  return trimmed !== "" && Number.isFinite(Number(trimmed));
}

function toNumber(value) {
  return isNumericValue(value) ? Number(value) : 0;
}

function findMessage(messageId) {
  for (const chat of state.chats) {
    const found = chat.messages.find(message => message.id === messageId);
    if (found) return found;
  }
  return null;
}

function downloadCSV(rows) {
  if (!rows.length) return toast("No rows to export", "error");
  const cols = Object.keys(rows[0]);
  const csv = [cols.join(","), ...rows.map(row => cols.map(col => `"${String(row[col] ?? "").replace(/"/g, '""')}"`).join(","))].join("\n");
  const link = document.createElement("a");
  link.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
  link.download = "plainsql-result.csv";
  link.click();
  URL.revokeObjectURL(link.href);
  toast("CSV exported", "success");
}

async function writeClipboard(value, message) {
  if (!value) return toast("Nothing to copy", "error");
  try {
    await navigator.clipboard.writeText(value);
    toast(message, "success");
  } catch {
    toast("Clipboard permission denied", "error");
  }
}

function toast(message, type = "success") {
  const node = document.createElement("div");
  node.className = `toast ${type}`;
  node.textContent = message;
  dom.toasts.appendChild(node);
  setTimeout(() => node.remove(), 3200);
}

function scrollToBottom() {
  requestAnimationFrame(() => {
    dom.chatScroll.scrollTop = dom.chatScroll.scrollHeight;
  });
}

function escapeHtml(value) {
  return String(value ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
}

function escapeAttr(value) {
  return escapeHtml(value).replace(/`/g, "&#096;");
}

function escapeSelector(value) {
  if (window.CSS && typeof window.CSS.escape === "function") return window.CSS.escape(String(value));
  return String(value).replace(/["\\]/g, "\\$&");
}

function stripMarkdown(value) {
  return value.replace(/\*\*([^*]+)\*\*/g, "$1").replace(/`([^`]+)`/g, "$1");
}

function formatCell(value) {
  if (value === null || typeof value === "undefined") return "";
  if (typeof value === "number") return Number.isInteger(value) ? String(value) : value.toLocaleString(undefined, { maximumFractionDigits: 3 });
  return String(value);
}

function highlightSQL(sql) {
  const keywords = ["select", "from", "where", "join", "left", "right", "inner", "outer", "on", "and", "or", "not", "in", "as", "order", "by", "group", "having", "limit", "desc", "asc", "count", "sum", "avg", "min", "max", "distinct", "between", "like", "is", "null", "with", "case", "when", "then", "else", "end", "round", "coalesce"];
  const keywordPattern = keywords.join("|");
  const tokenPattern = new RegExp(`('(?:''|[^'])*')|(\\b\\d+(?:\\.\\d+)?\\b)|(\\b(?:${keywordPattern})\\b)`, "gi");
  const source = String(sql ?? "");
  let html = "";
  let cursor = 0;
  for (const match of source.matchAll(tokenPattern)) {
    html += escapeHtml(source.slice(cursor, match.index));
    const safe = escapeHtml(match[0]);
    if (/^'/.test(match[0])) html += `<span class="str">${safe}</span>`;
    else if (/^\d/.test(match[0])) html += `<span class="num">${safe}</span>`;
    else html += `<span class="kw">${safe}</span>`;
    cursor = match.index + match[0].length;
  }
  return html + escapeHtml(source.slice(cursor));
}

function titleFromPrompt(prompt) {
  const compact = prompt.replace(/\s+/g, " ").trim();
  return compact.length > 52 ? `${compact.slice(0, 49)}...` : compact;
}

function readableLabel(value) {
  return String(value || "value").replace(/_/g, " ").replace(/\b\w/g, char => char.toUpperCase());
}

function createId(prefix) {
  return `${prefix}_${Math.random().toString(36).slice(2, 9)}_${Date.now().toString(36)}`;
}

function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

document.addEventListener("click", handleClick);
document.addEventListener("change", handleChange);
dom.form.addEventListener("submit", handleSubmit);
dom.input.addEventListener("keydown", handleInputKeydown);
dom.input.addEventListener("input", autoSizeInput);

render();
loadHealth();
loadSchema();
dom.input.focus();
