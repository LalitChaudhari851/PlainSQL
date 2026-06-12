import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Plus, MessageSquare, Bookmark, Trash2, X, Database, ChevronDown, ChevronRight, Sparkles } from 'lucide-react';
import useChatStore from '../../store/useChatStore';

function SectionHeader({ title, count, collapsed, onToggle }) {
  return (
    <button
      onClick={onToggle}
      className="flex items-center justify-between px-3 py-2 w-full group"
    >
      <div className="flex items-center gap-1.5">
        {onToggle && (
          <ChevronRight
            size={11}
            className={`text-t4 transition-transform duration-200 ${!collapsed ? 'rotate-90' : ''}`}
          />
        )}
        <span className="text-xs font-semibold uppercase tracking-wider text-t4 group-hover:text-t3 transition-colors">{title}</span>
      </div>
      {count != null && (
        <span className="text-xs text-t4 font-mono tabular-nums">{count}</span>
      )}
    </button>
  );
}

function EmptyChats({ onNewChat }) {
  return (
    <div className="px-4 py-6 text-center">
      <div className="w-10 h-10 mx-auto mb-3 rounded-xl flex items-center justify-center"
        style={{ background: 'var(--surface-2)', border: '1px solid var(--border-1)' }}>
        <Sparkles size={16} className="text-t3" />
      </div>
      <p className="text-t3 text-xs mb-1 font-medium">No conversations yet</p>
      <p className="text-t4 text-xs mb-3">Start your first AI-powered analysis</p>
      <button
        onClick={onNewChat}
        className="text-xs font-medium px-3 py-1.5 rounded-lg transition-colors"
        style={{ color: '#60a5fa', background: 'rgba(59,130,246,0.1)', border: '1px solid rgba(59,130,246,0.2)' }}
      >
        New chat
      </button>
    </div>
  );
}

function timeAgo(timestamp) {
  if (!timestamp) return '';
  const diff = Date.now() - timestamp;
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'now';
  if (mins < 60) return `${mins}m`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h`;
  return `${Math.floor(hrs / 24)}d`;
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
  const [schemaCollapsed, setSchemaCollapsed] = useState(false);
  const [savedCollapsed, setSavedCollapsed] = useState(false);

  const handleSavedQuery = (query) => {
    window.dispatchEvent(new CustomEvent('plainsql:submit', { detail: { query } }));
    if (open) onClose?.();
  };

  const handleNewChat = () => {
    newChat();
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
          border-r
          fixed lg:relative z-40 lg:z-auto
          transition-transform lg:translate-x-0
          ${open ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'}
        `}
        style={{
          background: 'rgba(6,9,18,0.97)',
          backdropFilter: 'blur(24px)',
          borderColor: 'var(--border-1)',
        }}
      >
        {/* Brand + Mobile close */}
        <div className="flex items-center gap-3 px-4 py-4" style={{ borderBottom: '1px solid var(--border-1)' }}>
          <div className="w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0 shadow-md"
            style={{ background: 'linear-gradient(135deg, #3b82f6, #06b6d4)' }}>
            <span className="text-white font-black text-sm">S</span>
          </div>
          <div className="flex-1 min-w-0">
            <p className="font-bold text-white text-sm leading-tight">PlainSQL</p>
            <p className="text-t4 text-xs">AI Data Copilot</p>
          </div>
          {/* Mobile close button */}
          <button
            onClick={onClose}
            className="lg:hidden w-7 h-7 rounded-lg flex items-center justify-center transition-colors hover:bg-white/10 focus-ring"
            aria-label="Close sidebar"
          >
            <X size={14} className="text-t3" />
          </button>
          {/* New chat */}
          <button
            onClick={handleNewChat}
            className="hidden lg:flex w-7 h-7 rounded-lg items-center justify-center transition-colors hover:bg-white/10 focus-ring"
            title="New chat"
            aria-label="New chat"
          >
            <Plus size={14} className="text-t3" />
          </button>
        </div>

        {/* Scrollable body */}
        <div className="flex-1 overflow-y-auto scrollbar-none py-2">

          {/* New Chat Button */}
          <div className="px-3 pb-3">
            <button
              onClick={handleNewChat}
              className="w-full flex items-center gap-2 px-3 py-2.5 rounded-xl text-sm font-medium text-t2 transition-all hover:text-white focus-ring"
              style={{
                background: 'var(--surface-1)',
                border: '1px dashed var(--border-2)',
              }}
            >
              <Plus size={14} />
              New chat
            </button>
          </div>

          {/* Schema */}
          <SectionHeader
            title="Schema"
            count={`${schemaTables.length} tables`}
            collapsed={schemaCollapsed}
            onToggle={() => setSchemaCollapsed(v => !v)}
          />
          <AnimatePresence initial={false}>
            {!schemaCollapsed && (
              <motion.div
                initial={{ height: 0, opacity: 0 }}
                animate={{ height: 'auto', opacity: 1 }}
                exit={{ height: 0, opacity: 0 }}
                className="overflow-hidden"
              >
                <div className="px-3 pb-3">
                  {/* All tables button */}
                  <button
                    onClick={() => { setSelectedSchema('default'); addToast('Context: All tables', 'info'); }}
                    className={`w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm text-left transition-all mb-1 ${
                      selectedSchema === 'default'
                        ? 'bg-primary/10 text-white font-medium border border-primary/20'
                        : 'text-t2 hover:bg-white/[0.04] border border-transparent'
                    }`}
                  >
                    <Database size={12} className={selectedSchema === 'default' ? 'text-primary' : 'text-t4'} />
                    All tables
                    <span className="ml-auto text-xs text-t4 font-mono tabular-nums">{schemaTables.length}</span>
                  </button>

                  {/* Scrollable table list */}
                  <div className="overflow-y-auto scrollbar-none space-y-0.5 rounded-lg p-1"
                    style={{ background: 'var(--surface-1)', border: '1px solid var(--border-1)' }}>
                    {schemaTables.map(t => (
                      <button
                        key={t}
                        onClick={() => { setSelectedSchema(t); addToast(`Context: ${t}`, 'info'); }}
                        className={`w-full flex items-center gap-2 px-2.5 py-1.5 rounded-md text-xs text-left transition-all ${
                          selectedSchema === t
                            ? 'bg-primary/10 text-white font-medium'
                            : 'text-t3 hover:bg-white/[0.05] hover:text-t2'
                        }`}
                      >
                        <span className="w-1 h-1 rounded-full flex-shrink-0"
                          style={{ background: selectedSchema === t ? '#3b82f6' : 'rgba(255,255,255,0.15)' }} />
                        {t}
                      </button>
                    ))}
                  </div>
                  <p className="text-t4 text-xs mt-1.5 px-1 flex items-center gap-1">
                    <Database size={9} />
                    Read-only access · AI-indexed
                  </p>
                </div>
              </motion.div>
            )}
          </AnimatePresence>

          {/* History */}
          <SectionHeader title="History" count={chats.length || null} />
          <div className="px-2 space-y-0.5 pb-3">
            {chats.length === 0 && (
              <EmptyChats onNewChat={handleNewChat} />
            )}
            {chats.map(chat => {
              const isActive = chat.id === activeChatId;
              const queryCount = chat.messages.filter(m => m.role === 'user').length;
              return (
                <motion.div
                  key={chat.id}
                  layout
                  onHoverStart={() => setHoveredChat(chat.id)}
                  onHoverEnd={() => setHoveredChat(null)}
                  onClick={() => { selectChat(chat.id); if (open) onClose?.(); }}
                  className={`
                    group flex items-center gap-2 px-3 py-2.5 rounded-xl cursor-pointer transition-all relative
                    ${isActive
                      ? 'bg-primary/10 border border-primary/20'
                      : 'hover:bg-white/[0.04] border border-transparent'
                    }
                  `}
                >
                  {/* Active indicator bar */}
                  {isActive && (
                    <div className="absolute left-0 top-1/2 -translate-y-1/2 w-0.5 h-5 rounded-full bg-primary" />
                  )}
                  <MessageSquare size={13} className={isActive ? 'text-primary flex-shrink-0' : 'text-t4 flex-shrink-0'} />
                  <div className="flex-1 min-w-0">
                    <span className={`text-xs truncate block ${isActive ? 'text-white font-medium' : 'text-t2'}`}>
                      {chat.title}
                    </span>
                  </div>
                  {queryCount > 0 && hoveredChat !== chat.id && (
                    <span className="text-t4 text-xs font-mono flex-shrink-0 tabular-nums">{queryCount}</span>
                  )}
                  {hoveredChat === chat.id && (
                    <button
                      onClick={e => { e.stopPropagation(); deleteChat(chat.id); }}
                      className="flex-shrink-0 w-5 h-5 rounded flex items-center justify-center hover:bg-danger/20 transition-colors"
                      aria-label={`Delete chat: ${chat.title}`}
                    >
                      <Trash2 size={11} className="text-t4 hover:text-danger" />
                    </button>
                  )}
                </motion.div>
              );
            })}
          </div>

          {/* Saved queries */}
          <SectionHeader
            title="Saved"
            count={savedQueries.length || null}
            collapsed={savedCollapsed}
            onToggle={() => setSavedCollapsed(v => !v)}
          />
          <AnimatePresence initial={false}>
            {!savedCollapsed && (
              <motion.div
                initial={{ height: 0, opacity: 0 }}
                animate={{ height: 'auto', opacity: 1 }}
                exit={{ height: 0, opacity: 0 }}
                className="overflow-hidden"
              >
                <div className="px-2 space-y-0.5 pb-4">
                  {savedQueries.map((q, i) => (
                    <button
                      key={i}
                      onClick={() => handleSavedQuery(q)}
                      className="w-full flex items-start gap-2 px-3 py-2 rounded-xl text-left transition-all group/saved focus-ring"
                      style={{ background: 'transparent' }}
                      onMouseEnter={e => { e.currentTarget.style.background = 'var(--surface-hover)'; }}
                      onMouseLeave={e => { e.currentTarget.style.background = 'transparent'; }}
                    >
                      <Bookmark size={12} className="text-t4 mt-0.5 flex-shrink-0 group-hover/saved:text-accent transition-colors" />
                      <span className="text-xs text-t3 group-hover/saved:text-t2 transition-colors line-clamp-2 leading-relaxed">{q}</span>
                    </button>
                  ))}
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Footer — single health indicator */}
        <div style={{ borderTop: '1px solid var(--border-1)' }} className="p-3">
          <div className="flex items-center gap-2.5 px-2">
            <div className={`w-2 h-2 rounded-full flex-shrink-0 ${
              health.status === 'healthy' ? 'dot-online' :
              health.status === 'degraded' ? 'dot-warning' : 'bg-white/20'
            }`} />
            <span className="text-xs text-t3 flex-1">
              {health.status === 'healthy' ? 'All systems operational' :
               health.status === 'degraded' ? 'Degraded performance' : 'Connecting...'}
            </span>
            {health.latency && (
              <span className="text-xs font-mono text-t4 tabular-nums">{health.latency}ms</span>
            )}
          </div>
        </div>
      </motion.aside>
    </>
  );
}
