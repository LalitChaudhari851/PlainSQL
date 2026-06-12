import { useEffect, useRef, useState } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { ArrowUp, Loader2, Sparkles } from 'lucide-react';
import useChatStore from '../../store/useChatStore';

const QUICK_PROMPTS = [
  'ARR by segment this quarter',
  'Churn risk with open P1 tickets',
  'Slowest SQL executions this week',
];

export default function Composer({ onSubmit }) {
  const isSending = useChatStore(s => s.isSending);
  const [value, setValue] = useState('');
  const [isFocused, setIsFocused] = useState(false);
  const textareaRef = useRef(null);

  useEffect(() => {
    const ta = textareaRef.current;
    if (!ta) return;
    ta.style.height = 'auto';
    ta.style.height = `${Math.min(ta.scrollHeight, 150)}px`;
  }, [value]);

  useEffect(() => {
    const handler = (e) => {
      setValue(e.detail.query);
      textareaRef.current?.focus();
    };
    window.addEventListener('plainsql:submit', handler);
    return () => window.removeEventListener('plainsql:submit', handler);
  }, []);

  const handleSubmit = (e) => {
    e?.preventDefault();
    const q = value.trim();
    if (!q || isSending) return;
    onSubmit(q);
    setValue('');
    if (textareaRef.current) textareaRef.current.style.height = 'auto';
  };

  const canSend = value.trim().length > 0 && !isSending;

  return (
    <div
      className="flex-shrink-0 px-4 pb-4 pt-3"
      style={{
        background: 'rgba(6,9,18,0.9)',
        backdropFilter: 'blur(20px)',
        borderTop: '1px solid var(--border-1)',
      }}
    >
      <div className="mx-auto max-w-5xl">
        {/* Quick prompts — always visible, dimmed when sending */}
        <div className={`mb-2 flex gap-2 overflow-x-auto scrollbar-none transition-opacity duration-200 ${isSending ? 'opacity-30 pointer-events-none' : ''}`}>
          {isSending && (
            <motion.div
              initial={{ opacity: 0, width: 0 }}
              animate={{ opacity: 1, width: 'auto' }}
              className="flex items-center gap-2 flex-shrink-0 pr-2"
            >
              <Loader2 size={12} className="animate-spin text-blue-400" />
              <span className="text-xs text-t3 whitespace-nowrap">Analyzing...</span>
            </motion.div>
          )}
          {QUICK_PROMPTS.map(prompt => (
            <button
              key={prompt}
              type="button"
              onClick={() => setValue(prompt)}
              disabled={isSending}
              className="flex-shrink-0 rounded-full px-3 py-1.5 text-xs text-t3 transition-all hover:text-t2 disabled:cursor-not-allowed"
              style={{
                background: 'var(--surface-1)',
                border: '1px solid var(--border-1)',
              }}
              onMouseEnter={e => {
                if (!isSending) {
                  e.currentTarget.style.borderColor = 'rgba(6,182,212,0.25)';
                  e.currentTarget.style.background = 'rgba(6,182,212,0.06)';
                }
              }}
              onMouseLeave={e => {
                e.currentTarget.style.borderColor = 'var(--border-1)';
                e.currentTarget.style.background = 'var(--surface-1)';
              }}
            >
              {prompt}
            </button>
          ))}
        </div>

        <form onSubmit={handleSubmit}>
          <div
            className={`flex items-end gap-3 rounded-2xl px-4 py-3 transition-all duration-200 ${isFocused ? 'border-glow' : ''}`}
            style={{
              background: 'var(--surface-2)',
              border: isFocused || canSend
                ? '1px solid rgba(59,130,246,0.3)'
                : '1px solid var(--border-2)',
              boxShadow: isFocused
                ? '0 0 0 3px rgba(59,130,246,0.08), 0 8px 32px rgba(0,0,0,0.2)'
                : '0 8px 32px rgba(0,0,0,0.15)',
            }}
          >
            <div className="mb-1 flex h-7 w-7 flex-shrink-0 items-center justify-center rounded-lg"
              style={{ background: 'var(--surface-1)', border: '1px solid var(--border-1)' }}>
              <Sparkles size={13} className={value ? 'text-cyan-300' : 'text-t4'} />
            </div>
            <textarea
              id="composer-input"
              ref={textareaRef}
              value={value}
              onChange={e => setValue(e.target.value)}
              onFocus={() => setIsFocused(true)}
              onBlur={() => setIsFocused(false)}
              onKeyDown={e => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault();
                  handleSubmit();
                }
              }}
              disabled={isSending}
              rows={1}
              placeholder="Ask a revenue, support, product, or pipeline question..."
              className="min-h-[24px] flex-1 resize-none bg-transparent text-sm leading-relaxed text-white outline-none placeholder:text-t4 disabled:opacity-50"
              aria-label="Type your query"
            />
            <motion.button
              type="submit"
              disabled={!canSend}
              whileHover={canSend ? { scale: 1.05 } : {}}
              whileTap={canSend ? { scale: 0.95 } : {}}
              className="flex h-9 w-9 flex-shrink-0 items-center justify-center rounded-xl transition-all duration-200 disabled:opacity-25"
              style={{
                background: canSend
                  ? 'linear-gradient(135deg, #2563eb, #0891b2)'
                  : 'var(--surface-2)',
                boxShadow: canSend
                  ? '0 4px 16px rgba(59,130,246,0.3)'
                  : 'none',
              }}
              aria-label="Send query"
            >
              <ArrowUp size={15} className="text-white" />
            </motion.button>
          </div>
        </form>

        <div className="mt-1.5 flex justify-between px-1">
          <span className="text-xs text-t4">
            Enter to run · Shift+Enter for new line
          </span>
          <span className="hidden text-xs text-t5 sm:inline">
            Validated SQL · Read-only
          </span>
        </div>
      </div>
    </div>
  );
}
