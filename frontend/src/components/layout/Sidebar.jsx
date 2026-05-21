import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Plus, MessageSquare, Bookmark, Trash2, ChevronRight } from 'lucide-react';
import useChatStore from '../../store/useChatStore';

function SectionHeader({ title, count }) {
  return (
    <div className="flex items-center justify-between px-3 py-2">
      <span className="text-xs font-semibold uppercase tracking-wider text-white/30">{title}</span>
      {count != null && <span className="text-xs text-white/20 font-mono">{count}</span>}
    </div>
  );
}

function StatusPanel({ health }) {
  const statusColor = health.status === 'healthy' ? 'dot-online' :
                      health.status === 'degraded' ? 'dot-warning' : 'bg-white/30';
  const statusLabel = health.status === 'healthy' ? 'All systems operational' :
                      health.status === 'degraded' ? 'Degraded performance' : 'Connecting...';

  return (
    <div className="mx-3 mb-3 flex items-center gap-2.5 rounded-xl px-3 py-2.5" style={{ background: 'rgba(255,255,255,0.03)', border: '1px solid rgba(255,255,255,0.06)' }}>
      <div className={`w-2 h-2 rounded-full flex-shrink-0 ${statusColor}`} />
      <span className="text-xs text-white/50">{statusLabel}</span>
      {health.latency && (
        <span className="ml-auto text-xs font-mono text-white/25">{health.latency}ms</span>
      )}
    </div>
  );
}

export default function Sidebar({ open, onClose }) {
  const chats = useChatStore(s => s.chats);
  const activeChatId = useChatStore(s => s.activeChatId);
  const savedQueries = useChatStore(s => s.savedQueries);
  const health = useChatStore(s => s.health);
  const schemaTables = useChatStore(s => s.schemaTables);
  const selectedSchema = useChatStore(s => s.selectedSchema);
  const newChat = useChatStore(s => s.newChat);
  const selectChat = useChatStore(s => s.selectChat);
  const deleteChat = useChatStore(s => s.deleteChat);
  const setSelectedSchema = useChatStore(s => s.setSelectedSchema);
  const addToast = useChatStore(s => s.addToast);

  const [hoveredChat, setHoveredChat] = useState(null);

  const handleSavedQuery = (query) => {
    // Dispatched upward via custom event
    window.dispatchEvent(new CustomEvent('plainsql:submit', { detail: { query } }));
    if (open) onClose?.();
  };

  return (
    <>
      {/* Mobile scrim */}
      <AnimatePresence>
        {open && (
          <motion.div
            key="scrim"
            initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
            onClick={onClose}
            className="fixed inset-0 z-30 bg-black/60 backdrop-blur-sm lg:hidden"
          />
        )}
      </AnimatePresence>

      <motion.aside
        initial={false}
        animate={{ x: open ? 0 : undefined }}
        className={`
          flex flex-col h-full w-[var(--sidebar-w)] flex-shrink-0
          border-r border-white/[0.06]
          fixed lg:relative z-40 lg:z-auto
          transition-transform lg:translate-x-0
          ${open ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'}
        `}
        style={{ background: 'rgba(6,9,18,0.95)', backdropFilter: 'blur(24px)' }}
      >
        {/* Brand */}
        <div className="flex items-center gap-3 px-4 py-4 border-b border-white/[0.06]">
          <div className="w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0"
            style={{ background: 'linear-gradient(135deg, #3b82f6, #06b6d4)' }}>
            <span className="text-white font-black text-sm">S</span>
          </div>
          <div className="flex-1 min-w-0">
            <p className="font-bold text-white text-sm leading-tight">PlainSQL</p>
            <p className="text-white/35 text-xs">AI Data Copilot</p>
          </div>
          <button
            onClick={() => { newChat(); if (open) onClose?.(); }}
            className="w-7 h-7 rounded-lg flex items-center justify-center transition-colors hover:bg-white/10 focus-ring"
            title="New chat"
          >
            <Plus size={14} className="text-white/50" />
          </button>
        </div>

        {/* Scrollable body */}
        <div className="flex-1 overflow-y-auto scrollbar-none py-2">

          {/* New Chat Button */}
          <div className="px-3 pb-3">
            <button
              onClick={() => { newChat(); if (open) onClose?.(); }}
              className="w-full flex items-center gap-2 px-3 py-2.5 rounded-xl text-sm font-medium text-white/70 transition-all hover:text-white hover:bg-white/[0.06] focus-ring"
              style={{ border: '1px dashed rgba(255,255,255,0.1)' }}
            >
              <Plus size={14} />
              New chat
            </button>
          </div>

          {/* Status panel */}
          <StatusPanel health={health} />

          {/* Schema */}
          <SectionHeader title="Schema" count={`${schemaTables.length} tables`} />
          <div className="px-3 pb-3">
            <select
              value={selectedSchema}
              onChange={e => { setSelectedSchema(e.target.value); addToast(`Context: ${e.target.value}`, 'info'); }}
              className="w-full bg-white/[0.04] border border-white/[0.08] rounded-xl px-3 py-2 text-sm text-white/80 focus-ring appearance-none"
              style={{ backgroundImage: "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%2394a3b8' stroke-width='2'%3E%3Cpath d='m6 9 6 6 6-6'/%3E%3C/svg%3E\")", backgroundRepeat: 'no-repeat', backgroundPosition: 'right 10px center', paddingRight: '28px' }}
            >
              <option value="default">All tables</option>
              {schemaTables.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
            <p className="text-white/25 text-xs mt-1.5 px-1">Read-only access · AI-indexed</p>
          </div>

          {/* History */}
          <SectionHeader title="History" count={chats.length || null} />
          <div className="px-2 space-y-0.5 pb-3">
            {chats.length === 0 && (
              <p className="text-white/25 text-xs px-3 py-2">No conversations yet</p>
            )}
            {chats.map(chat => (
              <motion.div
                key={chat.id}
                layout
                onHoverStart={() => setHoveredChat(chat.id)}
                onHoverEnd={() => setHoveredChat(null)}
                onClick={() => { selectChat(chat.id); if (open) onClose?.(); }}
                className={`
                  group flex items-center gap-2 px-3 py-2.5 rounded-xl cursor-pointer transition-all
                  ${chat.id === activeChatId
                    ? 'bg-primary/10 border border-primary/20'
                    : 'hover:bg-white/[0.04] border border-transparent'
                  }
                `}
              >
                <MessageSquare size={13} className={chat.id === activeChatId ? 'text-primary' : 'text-white/30'} />
                <span className={`flex-1 text-xs truncate ${chat.id === activeChatId ? 'text-white font-medium' : 'text-white/55'}`}>
                  {chat.title}
                </span>
                {chat.messages.length > 0 && (
                  <span className="text-white/20 text-xs font-mono flex-shrink-0">{chat.messages.filter(m=>m.role==='user').length}</span>
                )}
                {hoveredChat === chat.id && (
                  <button
                    onClick={e => { e.stopPropagation(); deleteChat(chat.id); }}
                    className="flex-shrink-0 w-5 h-5 rounded flex items-center justify-center hover:bg-danger/20 transition-colors"
                  >
                    <Trash2 size={11} className="text-white/30 hover:text-danger" />
                  </button>
                )}
              </motion.div>
            ))}
          </div>

          {/* Saved queries */}
          <SectionHeader title="Saved" count={savedQueries.length || null} />
          <div className="px-2 space-y-0.5 pb-4">
            {savedQueries.map((q, i) => (
              <button
                key={i}
                onClick={() => handleSavedQuery(q)}
                className="w-full flex items-start gap-2 px-3 py-2 rounded-xl text-left hover:bg-white/[0.04] transition-all group focus-ring"
              >
                <Bookmark size={12} className="text-white/25 mt-0.5 flex-shrink-0 group-hover:text-accent" />
                <span className="text-xs text-white/45 group-hover:text-white/70 transition-colors line-clamp-2 leading-relaxed">{q}</span>
              </button>
            ))}
          </div>
        </div>

        {/* Footer */}
        <div className="border-t border-white/[0.06] p-3">
          <div className="flex items-center gap-2 px-2">
            <div className={`w-2 h-2 rounded-full ${health.status === 'healthy' ? 'dot-online' : health.status === 'degraded' ? 'dot-warning' : 'bg-white/20'}`} />
            <span className="text-xs text-white/40">
              {health.status === 'healthy' ? 'All systems healthy' : health.status === 'degraded' ? 'Degraded mode' : 'Connecting...'}
            </span>
          </div>
        </div>
      </motion.aside>
    </>
  );
}
