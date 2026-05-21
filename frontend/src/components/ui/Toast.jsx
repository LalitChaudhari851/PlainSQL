import { motion, AnimatePresence } from 'framer-motion';
import useChatStore from '../../store/useChatStore';

export default function Toast() {
  const toasts = useChatStore(s => s.toasts);
  const removeToast = useChatStore(s => s.removeToast);

  const colorMap = {
    success: { bar: '#22c55e', bg: 'rgba(34,197,94,0.1)', border: 'rgba(34,197,94,0.25)' },
    error:   { bar: '#ef4444', bg: 'rgba(239,68,68,0.1)',  border: 'rgba(239,68,68,0.25)' },
    info:    { bar: '#3b82f6', bg: 'rgba(59,130,246,0.1)', border: 'rgba(59,130,246,0.25)' },
  };

  return (
    <div className="fixed bottom-6 right-6 z-50 flex flex-col gap-2 pointer-events-none">
      <AnimatePresence>
        {toasts.map(t => {
          const c = colorMap[t.type] ?? colorMap.info;
          return (
            <motion.div
              key={t.id}
              initial={{ opacity: 0, y: 12, scale: 0.96 }}
              animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={{ opacity: 0, y: -8, scale: 0.96 }}
              transition={{ duration: 0.2 }}
              onClick={() => removeToast(t.id)}
              className="pointer-events-auto flex items-center gap-3 rounded-xl px-4 py-3 text-sm font-medium cursor-pointer shadow-xl"
              style={{ background: c.bg, border: `1px solid ${c.border}`, backdropFilter: 'blur(16px)', minWidth: 220 }}
            >
              <div className="w-1 h-8 rounded-full flex-shrink-0" style={{ background: c.bar }} />
              <span className="text-white/90">{t.message}</span>
            </motion.div>
          );
        })}
      </AnimatePresence>
    </div>
  );
}
