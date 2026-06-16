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
    const steps = [15, 35, 60, 85, 100];
    for (const p of steps) {
      setProgress(p);
      await new Promise(r => setTimeout(r, 70));
    }

    // Health check — fire-and-forget
    fetchHealth()
      .then(h => setHealth({ status: h.status === 'healthy' ? 'healthy' : 'degraded', latency: null }))
      .catch(() => setHealth({ status: 'degraded', latency: null }));

    await new Promise(r => setTimeout(r, 150));
    setExiting(true);
    await new Promise(r => setTimeout(r, 300));
    setBooted();
  }, [clicked, setBooted, setHealth]);

  // Auto-launch on mount
  useEffect(() => {
    const timer = setTimeout(() => {
      handleLaunch();
    }, 100);
    return () => clearTimeout(timer);
  }, [handleLaunch]);

  return (
    <AnimatePresence>
      {!exiting && (
        <motion.div
          key="boot"
          initial={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.35, ease: 'easeOut' }}
          className="fixed inset-0 z-50 flex items-center justify-center overflow-hidden"
          style={{ background: 'var(--surface-0)' }}
        >
          {/* Animated gradient orbs */}
          <div className="absolute inset-0 overflow-hidden pointer-events-none">
            <div
              className="gradient-orb"
              style={{
                width: 500, height: 500,
                top: '20%', left: '30%',
                background: 'radial-gradient(circle, rgba(99,102,241,0.15) 0%, transparent 70%)',
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

          {/* Subtle grid pattern */}
          <div className="absolute inset-0 bg-grid pointer-events-none opacity-30" />

          {/* Center card */}
          <motion.div
            initial={{ opacity: 0, y: 15, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            transition={{ duration: 0.5, ease: [0.16, 1, 0.3, 1] }}
            className="relative z-10 w-full max-w-sm mx-4"
          >
            <div
              className="glass rounded-2xl px-8 py-10 text-center"
              style={{ boxShadow: '0 0 80px rgba(99,102,241,0.06), 0 8px 32px rgba(0,0,0,0.35)' }}
            >
              {/* Logo icon */}
              <div className="flex flex-col items-center mb-7">
                <motion.div
                  initial={{ scale: 0.8, opacity: 0 }}
                  animate={{ scale: 1, opacity: 1 }}
                  transition={{ delay: 0.1, duration: 0.4, ease: [0.16, 1, 0.3, 1] }}
                  className="w-14 h-14 rounded-2xl flex items-center justify-center mb-5 shadow-lg border border-white/[0.05]"
                  style={{
                    background: 'linear-gradient(135deg, var(--brand), var(--brand-cyan))',
                    boxShadow: '0 8px 30px rgba(99,102,241,0.3)',
                  }}
                >
                  <svg width="26" height="26" viewBox="0 0 24 24" fill="none">
                    <ellipse cx="12" cy="6" rx="8" ry="3" stroke="white" strokeWidth="1.5"/>
                    <path d="M4 6v6c0 1.657 3.582 3 8 3s8-1.343 8-3V6" stroke="white" strokeWidth="1.5"/>
                    <path d="M4 12v6c0 1.657 3.582 3 8 3s8-1.343 8-3v-6" stroke="white" strokeWidth="1.5"/>
                  </svg>
                </motion.div>

                <motion.h1
                  initial={{ opacity: 0, y: 8 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.15 }}
                  className="text-white font-black text-2xl tracking-tight"
                >
                  Plain<span className="text-gradient">SQL</span>
                </motion.h1>
                <motion.p
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ delay: 0.2 }}
                  className="text-t3 text-xs mt-1.5 font-semibold uppercase tracking-wider"
                >
                  AI Data Platform
                </motion.p>

                <motion.div
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ delay: 0.25 }}
                  className="flex items-center gap-2 mt-3"
                >
                  <span
                    className="text-[10px] px-2.5 py-0.5 rounded-full font-mono font-bold"
                    style={{ background: 'var(--brand-dim)', color: 'var(--brand-light)', border: '1px solid rgba(99,102,241,0.2)' }}
                  >
                    v2.1
                  </span>
                  <span
                    className="text-[10px] px-2.5 py-0.5 rounded-full font-mono font-bold"
                    style={{ background: 'rgba(52,211,153,0.12)', color: '#34d399', border: '1px solid rgba(52,211,153,0.2)' }}
                  >
                    SECURE BUILD
                  </span>
                </motion.div>
              </div>

              {/* Progress and status */}
              <div className="py-3">
                <div className="w-full h-1 rounded-full overflow-hidden mb-3.5"
                  style={{ background: 'rgba(255,255,255,0.06)' }}
                >
                  <motion.div
                    className="h-full rounded-full"
                    style={{ background: 'linear-gradient(90deg, var(--brand), var(--brand-cyan))' }}
                    initial={{ width: '0%' }}
                    animate={{ width: `${progress}%` }}
                    transition={{ duration: 0.1, ease: 'easeOut' }}
                  />
                </div>
                <div className="flex items-center justify-center gap-2">
                  <div className="w-1.5 h-1.5 rounded-full bg-brand-light animate-pulse" />
                  <span className="text-t3 text-xs font-semibold select-none">Initializing secure sandbox...</span>
                </div>
              </div>

              {/* Footer info */}
              <div className="flex justify-center gap-4 mt-6 pt-4 border-t border-border-1">
                <span className="text-t4 text-[10px] font-bold uppercase tracking-wider">Automated RAG</span>
                <span className="text-t5 text-[10px]">•</span>
                <span className="text-t4 text-[10px] font-bold uppercase tracking-wider">SQL Sandboxed</span>
              </div>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
