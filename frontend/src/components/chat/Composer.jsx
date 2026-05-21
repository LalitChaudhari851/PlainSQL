import { useEffect, useRef, useState } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { ArrowUp, Loader2, Search, Sparkles } from 'lucide-react';
import useChatStore from '../../store/useChatStore';

const QUICK_PROMPTS = [
  'ARR by segment this quarter',
  'Churn risk with open P1 tickets',
  'Slowest SQL executions this week',
];

export default function Composer({ onSubmit }) {
  const isSending = useChatStore(s => s.isSending);
  const [value, setValue] = useState('');
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
    <div className="flex-shrink-0 border-t border-white/[0.06] px-4 pb-4 pt-3" style={{ background: 'rgba(6,9,18,0.86)', backdropFilter: 'blur(18px)' }}>
      <div className="mx-auto max-w-5xl">
        <AnimatePresence>
          {isSending ? (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              exit={{ opacity: 0, height: 0 }}
              className="mb-2 flex items-center gap-2 px-1"
            >
              <Loader2 size={12} className="animate-spin text-blue-400" />
              <span className="text-xs text-white/45">Analyzing your question...</span>
            </motion.div>
          ) : (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="mb-2 flex gap-2 overflow-x-auto scrollbar-none"
            >
              {QUICK_PROMPTS.map(prompt => (
                <button
                  key={prompt}
                  type="button"
                  onClick={() => setValue(prompt)}
                  className="flex-shrink-0 rounded-full border border-white/[0.07] bg-white/[0.035] px-3 py-1.5 text-xs text-white/42 transition-all hover:border-cyan-400/25 hover:text-white/72"
                >
                  {prompt}
                </button>
              ))}
            </motion.div>
          )}
        </AnimatePresence>

        <form onSubmit={handleSubmit}>
          <div
            className="flex items-end gap-3 rounded-2xl px-4 py-3 transition-all"
            style={{
              background: 'rgba(255,255,255,0.05)',
              border: canSend ? '1px solid rgba(59,130,246,0.3)' : '1px solid rgba(255,255,255,0.1)',
              boxShadow: '0 8px 32px rgba(0,0,0,0.2)',
            }}
          >
            <div className="mb-1 flex h-7 w-7 flex-shrink-0 items-center justify-center rounded-xl border border-white/[0.08] bg-black/20">
              {value ? <Search size={14} className="text-cyan-300" /> : <Sparkles size={14} className="text-white/32" />}
            </div>
            <textarea
              ref={textareaRef}
              value={value}
              onChange={e => setValue(e.target.value)}
              onKeyDown={e => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault();
                  handleSubmit();
                }
              }}
              disabled={isSending}
              rows={1}
              placeholder="Ask a revenue, support, product, or pipeline question..."
              className="min-h-[24px] flex-1 resize-none bg-transparent text-sm leading-relaxed text-white outline-none placeholder:text-white/28 disabled:opacity-50"
            />
            <motion.button
              type="submit"
              disabled={!canSend}
              whileHover={canSend ? { scale: 1.05 } : {}}
              whileTap={canSend ? { scale: 0.95 } : {}}
              className="flex h-9 w-9 flex-shrink-0 items-center justify-center rounded-xl transition-all disabled:opacity-30"
              style={{
                background: canSend ? 'linear-gradient(135deg, #2563eb, #0891b2)' : 'rgba(255,255,255,0.08)',
                boxShadow: canSend ? '0 4px 12px rgba(59,130,246,0.25)' : 'none',
              }}
            >
              <ArrowUp size={15} className="text-white" />
            </motion.button>
          </div>
        </form>

        <div className="mt-1.5 flex justify-between px-1">
          <span className="text-xs text-white/20">Enter to run. Shift+Enter for a new line.</span>
          <span className="hidden text-xs text-white/18 sm:inline">Validated SQL · Read-only</span>
        </div>
      </div>
    </div>
  );
}
