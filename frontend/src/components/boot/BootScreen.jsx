import { useState, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import useChatStore from '../../store/useChatStore';
import { fetchHealth } from '../../api/client';

export default function BootScreen() {
  const setBooted = useChatStore(s => s.setBooted);
  const setHealth = useChatStore(s => s.setHealth);
  const [clicked, setClicked] = useState(false);
  const [exiting, setExiting] = useState(false);

  const handleLaunch = useCallback(async () => {
    if (clicked) return;
    setClicked(true);

    // Health check — fire-and-forget, never blocks
    fetchHealth()
      .then(h => setHealth({ status: h.status === 'healthy' ? 'healthy' : 'degraded', latency: null }))
      .catch(() => setHealth({ status: 'degraded', latency: null }));

    // Brief transition then enter app
    await new Promise(r => setTimeout(r, 400));
    setExiting(true);
    await new Promise(r => setTimeout(r, 350));
    setBooted();
  }, [clicked, setBooted, setHealth]);

  return (
    <AnimatePresence>
      {!exiting && (
        <motion.div
          key="boot"
          initial={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.35, ease: 'easeOut' }}
          className="fixed inset-0 z-50 flex items-center justify-center"
          style={{ background: '#060912' }}
        >
          {/* Subtle background */}
          <div className="absolute inset-0 bg-grid pointer-events-none opacity-30" />
          <div className="absolute inset-0 pointer-events-none"
            style={{ background: 'radial-gradient(ellipse 60% 40% at 50% 45%, rgba(59,130,246,0.06) 0%, transparent 70%)' }} />

          {/* Center card */}
          <motion.div
            initial={{ opacity: 0, y: 16 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.4, ease: 'easeOut' }}
            className="relative z-10 w-full max-w-sm mx-4"
          >
            <div className="glass rounded-2xl px-8 py-10 text-center"
              style={{ boxShadow: '0 0 60px rgba(59,130,246,0.08), 0 0 0 1px rgba(255,255,255,0.06)' }}>

              {/* Logo */}
              <div className="flex flex-col items-center mb-6">
                <div
                  className="w-16 h-16 rounded-2xl flex items-center justify-center mb-5"
                  style={{ background: 'linear-gradient(135deg, #3b82f6, #06b6d4)' }}
                >
                  <svg width="30" height="30" viewBox="0 0 24 24" fill="none">
                    <ellipse cx="12" cy="6" rx="8" ry="3" stroke="white" strokeWidth="1.5"/>
                    <path d="M4 6v6c0 1.657 3.582 3 8 3s8-1.343 8-3V6" stroke="white" strokeWidth="1.5"/>
                    <path d="M4 12v6c0 1.657 3.582 3 8 3s8-1.343 8-3v-6" stroke="white" strokeWidth="1.5"/>
                  </svg>
                </div>

                <h1 className="text-white font-bold text-2xl tracking-tight">
                  Plain<span className="text-gradient">SQL</span>
                </h1>
                <p className="text-white/40 text-sm mt-1.5">Enterprise Text-to-SQL Engine</p>

                <div className="flex items-center gap-2 mt-3">
                  <span className="text-xs px-2.5 py-0.5 rounded-full font-mono"
                    style={{ background: 'rgba(59,130,246,0.14)', color: '#60a5fa', border: '1px solid rgba(59,130,246,0.28)' }}>
                    v2.0
                  </span>
                  <span className="text-xs px-2.5 py-0.5 rounded-full font-mono"
                    style={{ background: 'rgba(6,182,212,0.12)', color: '#22d3ee', border: '1px solid rgba(6,182,212,0.22)' }}>
                    PRODUCTION
                  </span>
                </div>
              </div>

              {/* CTA */}
              {!clicked ? (
                <motion.button
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                  onClick={handleLaunch}
                  className="w-full py-3 rounded-xl font-semibold text-white text-sm transition-all"
                  style={{ background: 'linear-gradient(135deg, #3b82f6, #06b6d4)', boxShadow: '0 4px 20px rgba(59,130,246,0.3)' }}
                >
                  Launch Workspace
                </motion.button>
              ) : (
                <motion.div
                  initial={{ opacity: 0 }} animate={{ opacity: 1 }}
                  className="py-3 text-center"
                >
                  <div className="flex items-center justify-center gap-2">
                    <div className="w-1.5 h-1.5 rounded-full bg-cyan-400 animate-pulse" />
                    <span className="text-white/50 text-sm">Connecting...</span>
                  </div>
                </motion.div>
              )}

              {/* Footer */}
              <div className="flex justify-center gap-4 mt-6 pt-4 border-t border-white/[0.05]">
                <span className="text-white/20 text-xs font-mono">AI-Powered</span>
                <span className="text-white/10 text-xs">·</span>
                <span className="text-white/20 text-xs font-mono">Context-Aware</span>
                <span className="text-white/10 text-xs">·</span>
                <span className="text-white/20 text-xs font-mono">Secure</span>
              </div>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
