import { create } from 'zustand';
import { createConversation } from '../api/client';

const uid = (prefix = 'id') => `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
const titleFromPrompt = (q) => q.slice(0, 42) + (q.length > 42 ? '...' : '');

function loadPersisted() {
  try {
    const raw = localStorage.getItem('plainsql_v2');
    if (!raw) return null;
    return JSON.parse(raw);
  } catch { return null; }
}

function persistState(chats, activeChatId, savedQueries) {
  try {
    const clean = chats.map(c => ({
      ...c,
      messages: c.messages.map(m => ({ ...m, streaming: false, pending: false }))
    }));
    localStorage.setItem('plainsql_v2', JSON.stringify({ chats: clean, activeChatId, savedQueries }));
  } catch { /* quota errors are non-fatal */ }
}

const persisted = loadPersisted();

const useChatStore = create((set, get) => ({
  // ── Boot ──────────────────────────────────────────────
  booted: false,
  setBooted: () => set({ booted: true }),

  // ── System status ──────────────────────────────────────
  health: { status: 'checking', latency: null },
  setHealth: (health) => set({ health }),

  schemaTables: [
    'departments',
    'employees',
    'plans',
    'products',
    'feature_catalog',
    'accounts',
    'contacts',
    'workspaces',
    'workspace_users',
    'subscriptions',
    'invoices',
    'payments',
    'opportunities',
    'product_usage_daily',
    'support_tickets',
    'ticket_events',
    'incidents',
    'query_audit_log',
  ],
  setSchemaTables: (tables) => set({ schemaTables: tables }),
  selectedSchema: 'default',
  setSelectedSchema: (s) => set({ selectedSchema: s }),

  // ── Toasts ─────────────────────────────────────────────
  toasts: [],
  addToast: (message, type = 'info') => {
    const id = uid('toast');
    set(s => ({ toasts: [...s.toasts, { id, message, type }] }));
    setTimeout(() => set(s => ({ toasts: s.toasts.filter(t => t.id !== id) })), 3500);
  },
  removeToast: (id) => set(s => ({ toasts: s.toasts.filter(t => t.id !== id) })),

  // ── Chats ──────────────────────────────────────────────
  chats: persisted?.chats ?? [],
  activeChatId: persisted?.activeChatId ?? null,
  savedQueries: persisted?.savedQueries ?? [
    'Show net revenue retention by customer segment for the last 4 quarters',
    'Which accounts have high ARR, low product usage, and unresolved critical tickets?',
    'Rank sales reps by closed-won ARR and average deal cycle length this year',
    'Show weekly query volume, failed executions, and p95 latency by workspace',
  ],
  isSending: false,
  setIsSending: (v) => set({ isSending: v }),

  getActiveChat: () => {
    const { chats, activeChatId } = get();
    return chats.find(c => c.id === activeChatId) ?? null;
  },

  newChat: () => {
    const id = uid('chat');
    const chat = { id, title: 'New analysis', messages: [], context: [] };
    set(s => {
      const next = [chat, ...s.chats];
      persistState(next, id, s.savedQueries);
      return { chats: next, activeChatId: id };
    });
    createConversation('New analysis');
  },

  selectChat: (id) => {
    set(s => { persistState(s.chats, id, s.savedQueries); return { activeChatId: id }; });
  },

  deleteChat: (id) => {
    set(s => {
      const next = s.chats.filter(c => c.id !== id);
      const active = s.activeChatId === id ? (next[0]?.id ?? null) : s.activeChatId;
      persistState(next, active, s.savedQueries);
      return { chats: next, activeChatId: active };
    });
  },

  // Ensure there's an active chat to receive messages
  ensureChat: (title) => {
    const { chats, activeChatId } = get();
    const existing = chats.find(c => c.id === activeChatId);
    if (existing) return existing;
    const id = uid('chat');
    const chat = { id, title: titleFromPrompt(title), messages: [], context: [] };
    set(s => {
      const next = [chat, ...s.chats];
      persistState(next, id, s.savedQueries);
      return { chats: next, activeChatId: id };
    });
    createConversation(chat.title);
    return chat;
  },

  // Push user + pending assistant message
  addUserMessage: (question) => {
    const chat = get().ensureChat(question);
    const userMsgId = uid('msg');
    const pendingId = uid('msg');
    const userMsg = { id: userMsgId, role: 'user', content: question };
    const pending = { id: pendingId, role: 'assistant', pending: true, data: null, _userQuery: question, _chatMode: false };

    // Update title if chat is new
    set(s => {
      const next = s.chats.map(c => {
        if (c.id !== chat.id) return c;
        const newTitle = c.messages.length === 0 ? titleFromPrompt(question) : c.title;
        return { ...c, title: newTitle, messages: [...c.messages, userMsg, pending] };
      });
      persistState(next, s.activeChatId, s.savedQueries);
      return { chats: next };
    });
    return { chat, pendingId };
  },

  // Update an in-flight message
  updateMessage: (chatId, msgId, patch) => {
    set(s => {
      const next = s.chats.map(c => {
        if (c.id !== chatId) return c;
        return { ...c, messages: c.messages.map(m => m.id === msgId ? { ...m, ...patch } : m) };
      });
      persistState(next, s.activeChatId, s.savedQueries);
      return { chats: next };
    });
  },

  // Push context entry after successful SQL
  pushContext: (chatId, entry) => {
    set(s => ({
      chats: s.chats.map(c => {
        if (c.id !== chatId) return c;
        const ctx = [...(c.context ?? []), entry].slice(-8);
        return { ...c, context: ctx };
      })
    }));
  },

  saveSqlQuery: (sql) => {
    if (!sql) return;
    set(s => {
      if (s.savedQueries.includes(sql)) return {};
      const next = [sql, ...s.savedQueries];
      persistState(s.chats, s.activeChatId, next);
      return { savedQueries: next };
    });
  },

  setFeedback: (chatId, msgId, rating) => {
    set(s => ({
      chats: s.chats.map(c => {
        if (c.id !== chatId) return c;
        return { ...c, messages: c.messages.map(m => m.id === msgId ? { ...m, _feedback: rating } : m) };
      })
    }));
  },
}));

export default useChatStore;
