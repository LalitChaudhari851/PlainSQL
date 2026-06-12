import { motion, AnimatePresence } from 'framer-motion';
import { CheckCircle2, XCircle, Info } from 'lucide-react';
import useChatStore from '../../store/useChatStore';

const icons = {
  success: CheckCircle2,
  error: XCircle,
  info: Info,
};

const colorMap = {
  success: {
    bar: '#22c55e',
    bg: 'rgba(34,197,94,0.08)',
    border: 'rgba(34,197,94,0.20)',
    icon: '#4ade80',
  },
  error: {
    bar: '#ef4444',
    bg: 'rgba(239,68,68,0.08)',
    border: 'rgba(239,68,68,0.20)',
    icon: '#f87171',
  },
  info: {
    bar: '#3b82f6',
    bg: 'rgba(59,130,246,0.08)',
    border: 'rgba(59,130,246,0.20)',
    icon: '#60a5fa',
  },
};

export default function Toast() {
  const toasts = useChatStore(s => s.toasts);
  const removeToast = useChatStore(s => s.removeToast);

  return (
    <div className="fixed top-4 right-4 z-50 flex flex-col gap-2 pointer-events-none max-w-sm w-full">
      <AnimatePresence>
        {toasts.map(t => {
          const c = colorMap[t.type] ?? colorMap.info;
          const Icon = icons[t.type] ?? icons.info;
          return (
            <motion.div
              key={t.id}
              initial={{ opacity: 0, x: 40, scale: 0.95 }}
              animate={{ opacity: 1, x: 0, scale: 1 }}
              exit={{ opacity: 0, x: 40, scale: 0.95 }}
              transition={{ duration: 0.25, ease: [0.16, 1, 0.3, 1] }}
              onClick={() => removeToast(t.id)}
              className="pointer-events-auto flex flex-col rounded-xl cursor-pointer shadow-2xl overflow-hidden"
              style={{
                background: c.bg,
                border: `1px solid ${c.border}`,
                backdropFilter: 'blur(20px)',
              }}
            >
              <div className="flex items-center gap-3 px-4 py-3">
                <Icon size={16} style={{ color: c.icon }} className="flex-shrink-0" />
                <span className="text-sm font-medium text-t1 flex-1">{t.message}</span>
              </div>
              {/* Auto-dismiss progress bar */}
              <div className="h-0.5 w-full" style={{ background: 'rgba(255,255,255,0.04)' }}>
                <div
                  className="h-full toast-progress rounded-full"
                  style={{ background: c.bar }}
                />
              </div>
            </motion.div>
          );
        })}
      </AnimatePresence>
    </div>
  );
}
