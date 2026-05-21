import { Menu, Plus, Zap } from 'lucide-react';
import useChatStore from '../../store/useChatStore';

export default function Topbar({ onMenuClick }) {
  const chat = useChatStore(s => s.getActiveChat());
  const health = useChatStore(s => s.health);
  const newChat = useChatStore(s => s.newChat);

  return (
    <header
      className="flex items-center gap-3 px-4 h-[var(--topbar-h)] flex-shrink-0 border-b border-white/[0.06]"
      style={{ background: 'rgba(6,9,18,0.8)', backdropFilter: 'blur(16px)' }}
    >
      {/* Mobile menu */}
      <button
        onClick={onMenuClick}
        className="lg:hidden w-8 h-8 rounded-lg flex items-center justify-center hover:bg-white/10 transition-colors focus-ring"
      >
        <Menu size={16} className="text-white/60" />
      </button>

      {/* Conversation title */}
      <div className="flex-1 min-w-0">
        <h1 className="text-sm font-semibold text-white truncate">
          {chat?.title ?? 'PlainSQL'}
        </h1>
        <p className="text-xs text-white/35 hidden sm:block">
          {chat?.messages?.length
            ? `${chat.messages.filter(m=>m.role==='user').length} queries in this session`
            : 'AI-assisted data workspace'}
        </p>
      </div>

      {/* Badges */}
      <div className="flex items-center gap-2">
        {/* Engine badge */}
        <div className="hidden sm:flex items-center gap-1.5 px-2.5 py-1 rounded-lg"
          style={{ background: 'rgba(59,130,246,0.08)', border: '1px solid rgba(59,130,246,0.15)' }}>
          <Zap size={11} className="text-primary" />
          <span className="text-xs text-primary/90 font-medium">AI Engine</span>
        </div>

        {/* Health dot */}
        <div className={`w-2 h-2 rounded-full ${
          health.status === 'healthy' ? 'dot-online' :
          health.status === 'degraded' ? 'dot-warning' : 'bg-white/20'
        }`} title={health.status} />

        {/* New Chat */}
        <button
          onClick={newChat}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium text-white/70 hover:text-white hover:bg-white/10 transition-all focus-ring border border-white/10"
        >
          <Plus size={12} />
          <span className="hidden sm:inline">New chat</span>
        </button>
      </div>
    </header>
  );
}
