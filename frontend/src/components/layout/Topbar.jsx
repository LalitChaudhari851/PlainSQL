import { Menu, Plus, Activity } from 'lucide-react';
import useChatStore from '../../store/useChatStore';

export default function Topbar({ onMenuClick }) {
  const chat = useChatStore(s => s.getActiveChat());
  const health = useChatStore(s => s.health);
  const newChat = useChatStore(s => s.newChat);
  const selectedSchema = useChatStore(s => s.selectedSchema);

  const statusText = health.status === 'healthy' ? 'All systems go' :
                     health.status === 'degraded' ? 'Degraded' : 'Connecting...';
  const statusDotClass = health.status === 'healthy' ? 'dot-online' :
                         health.status === 'degraded' ? 'dot-warning' : 'bg-white/20';

  const queryCount = chat?.messages?.filter(m => m.role === 'user').length ?? 0;

  return (
    <header
      className="flex items-center gap-3 px-4 h-[var(--topbar-h)] flex-shrink-0"
      style={{
        background: 'rgba(6,9,18,0.85)',
        backdropFilter: 'blur(16px)',
        borderBottom: '1px solid var(--border-1)',
      }}
    >
      {/* Mobile menu */}
      <button
        onClick={onMenuClick}
        className="lg:hidden w-8 h-8 rounded-lg flex items-center justify-center hover:bg-white/10 transition-colors focus-ring"
        aria-label="Open menu"
      >
        <Menu size={16} className="text-t2" />
      </button>

      {/* Conversation title */}
      <div className="flex-1 min-w-0">
        <h1 className="text-sm font-semibold text-white truncate">
          {chat?.title ?? 'PlainSQL'}
        </h1>
        <p className="text-xs text-t3 hidden sm:block">
          {queryCount > 0
            ? `${queryCount} ${queryCount === 1 ? 'query' : 'queries'}${selectedSchema !== 'default' ? ` · ${selectedSchema}` : ''}`
            : 'AI-assisted data workspace'}
        </p>
      </div>

      {/* Right section */}
      <div className="flex items-center gap-2.5">
        {/* Health status */}
        <div
          className="hidden sm:flex items-center gap-2 px-2.5 py-1.5 rounded-lg"
          style={{ background: 'var(--surface-1)', border: '1px solid var(--border-1)' }}
        >
          <div className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${statusDotClass}`} />
          <span className="text-xs text-t3 font-medium">{statusText}</span>
          {health.latency && (
            <span className="text-xs font-mono text-t4 tabular-nums">{health.latency}ms</span>
          )}
        </div>

        {/* Mobile health dot only */}
        <div className={`sm:hidden w-2 h-2 rounded-full ${statusDotClass}`}
          title={statusText}
          role="status"
          aria-label={statusText}
        />

        {/* New Chat */}
        <button
          onClick={newChat}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium text-t2 hover:text-white transition-all focus-ring"
          style={{
            background: 'var(--surface-1)',
            border: '1px solid var(--border-2)',
          }}
          title="New chat (Ctrl+N)"
          aria-label="New chat"
        >
          <Plus size={12} />
          <span className="hidden sm:inline">New</span>
        </button>
      </div>
    </header>
  );
}
