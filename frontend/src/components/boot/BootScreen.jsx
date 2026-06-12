import { useState, useCallback, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import useChatStore from '../../store/useChatStore';
import { fetchHealth } from '../../api/client';

export default function BootScreen() {
  const setBooted = useChatStore(s => s.setBooted);
  const setHealth = useChatStore(s => s.setHealth);
  const [clicked, setClicked] = useState(false);
  const [exiting, setExiting] = useState(false);
  const [progress, setProgress] = useState(0);

  const handleLaunch = useCallback(async () => {
    if (clicked) return;
    setClicked(true);

    // Animate progress bar
    const steps = [20, 45, 70, 90, 100];
    for (const p of steps) {
      setProgress(p);
      await new Promise(r => setTimeout(r, 80));
    }

    // Health check — fire-and-forget, never blocks
    fetchHealth()
      .then(h => setHealth({ status: h.status === 'healthy' ? 'healthy' : 'degraded', latency: null }))
      .catch(() => setHealth({ status: 'degraded', latency: null }));

    await new Promise(r => setTimeout(r, 200));
    setExiting(true);
    await new Promise(r => setTimeout(r, 350));
    setBooted();
  }, [clicked, setBooted, setHealth]);

  // Keyboard shortcut — Enter to launch
  useEffect(() => {
    const handler = (e) => {
      if (e.key === 'Enter') handleLaunch();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [handleLaunch]);

  return (
    <AnimatePresence>
      {!exiting && (
        <motion.div
          key="boot"
          initial={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.35, ease: 'easeOut' }}
          className="fixed inset-0 z-50 flex items-center justify-center"
          style={{ background: 'var(--surface-0)' }}
        >
          {/* Animated gradient orbs */}
          <div className="absolute inset-0 overflow-hidden pointer-events-none">
            <div
              className="gradient-orb"
              style={{
                width: 500, height: 500,
                top: '20%', left: '30%',
                background: 'radial-gradient(circle, rgba(59,130,246,0.15) 0%, transparent 70%)',
              }}
            />
            <div
              className="gradient-orb"
              style={{
                width: 400, height: 400,
                bottom: '10%', right: '20%',
                background: 'radial-gradient(circle, rgba(6,182,212,0.12) 0%, transparent 70%)',
                animationDelay: '-3s',
              }}
            />
            <div
              className="gradient-orb"
              style={{
                width: 300, height: 300,
                top: '50%', right: '40%',
                background: 'radial-gradient(circle, rgba(167,139,250,0.08) 0%, transparent 70%)',
                animationDelay: '-5s',
              }}
            />
          </div>

          {/* Subtle grid */}
          <div className="absolute inset-0 bg-grid pointer-events-none opacity-30" />

          {/* Center card */}
          <motion.div
            initial={{ opacity: 0, y: 20, scale: 0.97 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            transition={{ duration: 0.5, ease: [0.16, 1, 0.3, 1] }}
            className="relative z-10 w-full max-w-sm mx-4"
          >
            <div
              className="glass rounded-2xl px-8 py-10 text-center"
              style={{ boxShadow: '0 0 80px rgba(59,130,246,0.06), 0 8px 32px rgba(0,0,0,0.3)' }}
            >
              {/* Logo */}
              <div className="flex flex-col items-center mb-7">
                <motion.div
                  initial={{ scale: 0.8, opacity: 0 }}
                  animate={{ scale: 1, opacity: 1 }}
                  transition={{ delay: 0.1, duration: 0.4, ease: [0.16, 1, 0.3, 1] }}
                  className="w-16 h-16 rounded-2xl flex items-center justify-center mb-5 shadow-lg"
                  style={{
                    background: 'linear-gradient(135deg, #3b82f6, #06b6d4)',
                    boxShadow: '0 8px 30px rgba(59,130,246,0.3)',
                  }}
                >
                  <svg width="30" height="30" viewBox="0 0 24 24" fill="none">
                    <ellipse cx="12" cy="6" rx="8" ry="3" stroke="white" strokeWidth="1.5"/>
                    <path d="M4 6v6c0 1.657 3.582 3 8 3s8-1.343 8-3V6" stroke="white" strokeWidth="1.5"/>
                    <path d="M4 12v6c0 1.657 3.582 3 8 3s8-1.343 8-3v-6" stroke="white" strokeWidth="1.5"/>
                  </svg>
                </motion.div>

                <motion.h1
                  initial={{ opacity: 0, y: 8 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.2 }}
                  className="text-white font-bold text-2xl tracking-tight"
                >
                  Plain<span className="text-gradient">SQL</span>
                </motion.h1>
                <motion.p
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ delay: 0.3 }}
                  className="text-t3 text-sm mt-1.5"
                >
                  Enterprise Text-to-SQL Engine
                </motion.p>

                <motion.div
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ delay: 0.35 }}
                  className="flex items-center gap-2 mt-3"
                >
                  <span
                    className="text-xs px-2.5 py-0.5 rounded-full font-mono"
                    style={{ background: 'rgba(59,130,246,0.14)', color: '#60a5fa', border: '1px solid rgba(59,130,246,0.28)' }}
                  >
                    v2.0
                  </span>
                  <span
                    className="text-xs px-2.5 py-0.5 rounded-full font-mono"
                    style={{ background: 'rgba(6,182,212,0.12)', color: '#22d3ee', border: '1px solid rgba(6,182,212,0.22)' }}
                  >
                    PRODUCTION
                  </span>
                </motion.div>
              </div>

              {/* CTA */}
              {!clicked ? (
                <motion.button
                  initial={{ opacity: 0, y: 8 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.4 }}
                  whileHover={{ scale: 1.02, boxShadow: '0 8px 30px rgba(59,130,246,0.4)' }}
                  whileTap={{ scale: 0.98 }}
                  onClick={handleLaunch}
                  className="w-full py-3.5 rounded-xl font-semibold text-white text-sm transition-all"
                  style={{
                    background: 'linear-gradient(135deg, #3b82f6, #06b6d4)',
                    boxShadow: '0 4px 20px rgba(59,130,246,0.3)',
                  }}
                >
                  Launch Workspace
                  <span className="ml-2 text-white/50 text-xs">↵</span>
                </motion.button>
              ) : (
                <motion.div
                  initial={{ opacity: 0 }} animate={{ opacity: 1 }}
                  className="py-3"
                >
                  {/* Progress bar */}
                  <div className="w-full h-1.5 rounded-full overflow-hidden mb-3"
                    style={{ background: 'rgba(255,255,255,0.06)' }}
                  >
                    <motion.div
                      className="h-full rounded-full"
                      style={{ background: 'linear-gradient(90deg, #3b82f6, #06b6d4)' }}
                      initial={{ width: '0%' }}
                      animate={{ width: `${progress}%` }}
                      transition={{ duration: 0.15, ease: 'easeOut' }}
                    />
                  </div>
                  <div className="flex items-center justify-center gap-2">
                    <div className="w-1.5 h-1.5 rounded-full bg-cyan-400 animate-pulse" />
                    <span className="text-t3 text-sm">Initializing workspace...</span>
                  </div>
                </motion.div>
              )}

              {/* Footer */}
              <div className="flex justify-center gap-4 mt-6 pt-4" style={{ borderTop: '1px solid var(--border-1)' }}>
                <span className="text-t4 text-xs font-mono">AI-Powered</span>
                <span className="text-t5 text-xs">·</span>
                <span className="text-t4 text-xs font-mono">Context-Aware</span>
                <span className="text-t5 text-xs">·</span>
                <span className="text-t4 text-xs font-mono">Secure</span>
              </div>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
