import { Menu, Plus, PanelLeft, PanelLeftClose } from 'lucide-react';
import useChatStore from '../../store/useChatStore';

export default function Topbar({ onMenuClick }) {
  const chat = useChatStore(s => s.getActiveChat());
  const health = useChatStore(s => s.health);
  const newChat = useChatStore(s => s.newChat);
  const selectedSchema = useChatStore(s => s.selectedSchema);
  const sidebarCollapsed = useChatStore(s => s.sidebarCollapsed);
  const setSidebarCollapsed = useChatStore(s => s.setSidebarCollapsed);

  const statusText = health.status === 'healthy' ? 'Healthy' :
                     health.status === 'degraded' ? 'Degraded' : 'Connecting...';
  const statusDotClass = health.status === 'healthy' ? 'dot-online' :
                          health.status === 'degraded' ? 'dot-warning' : 'bg-white/20';

  const queryCount = chat?.messages?.filter(m => m.role === 'user').length ?? 0;

  return (
    <header
      className="flex items-center gap-3 px-4 h-[var(--topbar-h)] flex-shrink-0 relative z-20"
      style={{
        background: 'var(--surface-05)',
        borderBottom: '1px solid var(--border-1)',
      }}
    >
      {/* Sidebar toggle for desktop */}
      <button
        onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
        className="hidden lg:flex w-8 h-8 rounded-lg items-center justify-center text-t4 hover:text-t2 hover:bg-white/5 transition-colors focus-ring"
        title={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
        aria-label="Toggle sidebar"
      >
        {sidebarCollapsed ? <PanelLeft size={16} /> : <PanelLeftClose size={16} />}
      </button>

      {/* Mobile menu trigger */}
      <button
        onClick={onMenuClick}
        className="lg:hidden w-8 h-8 rounded-lg flex items-center justify-center text-t3 hover:text-t2 hover:bg-white/5 transition-colors focus-ring"
        aria-label="Open menu"
      >
        <Menu size={16} />
      </button>

      {/* Conversation title */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <h1 className="text-sm font-semibold text-white truncate">
            {chat?.title ?? 'New Analysis'}
          </h1>
          {selectedSchema !== 'default' && (
            <span className="text-[10px] font-semibold font-mono bg-brand-dim text-brand-light border border-brand/20 px-1.5 py-0.5 rounded uppercase">
              {selectedSchema}
            </span>
          )}
        </div>
        <p className="text-[11px] text-t4 hidden sm:block font-medium">
          {queryCount > 0
            ? `${queryCount} ${queryCount === 1 ? 'query' : 'queries'} in thread`
            : 'AI-assisted database intelligence'}
        </p>
      </div>

      {/* Right section */}
      <div className="flex items-center gap-2.5">
        {/* Health status indicator (minimal) */}
        <div
          className="hidden sm:flex items-center gap-2 px-2.5 py-1.2 rounded-lg bg-surface-1 border border-border-1 hover:border-border-2 transition-colors cursor-help"
          title={`Server latency: ${health.latency || 0}ms`}
        >
          <div className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${statusDotClass}`} />
          <span className="text-[11px] text-t3 font-medium select-none">{statusText}</span>
          {health.latency && (
            <span className="text-[10px] font-mono text-t4 tabular-nums select-none">{health.latency}ms</span>
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
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold text-t2 hover:text-white transition-all bg-surface-1 hover:bg-surface-2 border border-border-2 hover:border-brand/40 focus-ring"
          title="New chat (Ctrl+N)"
          aria-label="New chat"
        >
          <Plus size={13} className="text-brand-light" />
          <span className="hidden sm:inline">New</span>
        </button>
      </div>
    </header>
  );
}
