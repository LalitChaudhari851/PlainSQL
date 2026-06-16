import { motion } from 'framer-motion';
import { CheckCircle2, Code2, Loader2, Play, Search, ShieldCheck, Sparkles } from 'lucide-react';

const STEPS = [
  { key: 'retrieve',   label: 'Retrieve',   icon: Search },
  { key: 'generate',   label: 'Generate',   icon: Code2 },
  { key: 'validate',   label: 'Validate',   icon: ShieldCheck },
  { key: 'execute',    label: 'Execute',     icon: Play },
  { key: 'synthesize', label: 'Synthesize',  icon: Sparkles },
];

function StepDot({ step, isDone, isActive, stageText }) {
  const Icon = step.icon;
  return (
    <div className="flex items-center gap-1.5">
      <div
        className="grid h-5 w-5 place-items-center rounded-md transition-all duration-300"
        style={{
          background: isDone
            ? 'rgba(52,211,153,0.12)'
            : isActive
              ? 'var(--brand-dim)'
              : 'var(--surface-1)',
          border: `1px solid ${
            isDone
              ? 'rgba(52,211,153,0.25)'
              : isActive
                ? 'rgba(99,102,241,0.3)'
                : 'var(--border-1)'
          }`,
          boxShadow: isActive ? '0 0 8px rgba(99,102,241,0.15)' : 'none',
        }}
      >
        {isDone ? (
          <CheckCircle2 size={11} className="text-success" />
        ) : isActive ? (
          <motion.div animate={{ rotate: 360 }} transition={{ duration: 1, repeat: Infinity, ease: 'linear' }}>
            <Loader2 size={11} className="text-brand-light" />
          </motion.div>
        ) : (
          <Icon size={11} className="text-t4" />
        )}
      </div>
      <span className={`text-[11px] font-semibold transition-colors duration-200 ${
        isDone ? 'text-success/90' : isActive ? 'text-brand-light font-bold' : 'text-t4'
      }`}>
        {isActive && stageText ? stageText : step.label}
      </span>
    </div>
  );
}

function Connector({ isDone, isActive }) {
  return (
    <div className="hidden sm:block h-px flex-1 min-w-3 max-w-8 mx-0.5 relative overflow-hidden rounded-full"
      style={{ background: 'var(--border-1)' }}
    >
      <motion.div
        className="absolute inset-y-0 left-0 rounded-full"
        style={{ background: isDone ? 'rgba(52,211,153,0.4)' : 'rgba(99,102,241,0.4)' }}
        initial={{ width: '0%' }}
        animate={{ width: isDone || isActive ? '100%' : '0%' }}
        transition={{ duration: 0.4, ease: 'easeOut' }}
      />
    </div>
  );
}

export default function PipelineTrace({ activeStep = -1, isChatMode = false, stageText = '' }) {
  if (isChatMode) {
    return (
      <div className="mb-4 inline-flex items-center gap-1.5 rounded-lg px-2.5 py-1"
        style={{
          background: 'var(--brand-dim)',
          border: '1px solid rgba(99,102,241,0.2)',
        }}
      >
        <Sparkles size={11} className="text-brand-light" />
        <span className="text-[10px] font-bold uppercase tracking-wider text-brand-light">Conversational</span>
      </div>
    );
  }

  const allDone = activeStep >= STEPS.length;

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      className="mb-4 rounded-xl px-3 py-2.5"
      style={{
        background: 'var(--surface-1)',
        border: `1px solid ${allDone ? 'rgba(52,211,153,0.15)' : 'var(--border-1)'}`,
      }}
    >
      {/* Desktop: horizontal */}
      <div className="hidden sm:flex flex-wrap items-center gap-y-1.5">
        {STEPS.map((step, i) => {
          const isDone = i < activeStep || allDone;
          const isActive = i === activeStep && !allDone;

          return (
            <div key={step.key} className="contents">
              <StepDot step={step} isDone={isDone} isActive={isActive} stageText={isActive ? stageText : ''} />
              {i < STEPS.length - 1 && <Connector isDone={isDone} isActive={isActive} />}
            </div>
          );
        })}
        {allDone && (
          <motion.div
            initial={{ opacity: 0, scale: 0.8 }}
            animate={{ opacity: 1, scale: 1 }}
            className="ml-auto flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wide"
            style={{ background: 'rgba(52,211,153,0.1)', color: 'var(--success)', border: '1px solid rgba(52,211,153,0.2)' }}
          >
            <CheckCircle2 size={10} />
            Complete
          </motion.div>
        )}
      </div>

      {/* Mobile: vertical stack */}
      <div className="sm:hidden flex flex-col gap-1.5">
        {STEPS.map((step, i) => {
          const isDone = i < activeStep || allDone;
          const isActive = i === activeStep && !allDone;

          return (
            <div key={step.key} className="flex items-center gap-2">
              <div
                className="w-0.5 h-full min-h-[16px] rounded-full flex-shrink-0"
                style={{
                  background: isDone ? 'rgba(52,211,153,0.3)' : isActive ? 'rgba(99,102,241,0.3)' : 'var(--border-1)',
                }}
              />
              <StepDot step={step} isDone={isDone} isActive={isActive} stageText={isActive ? stageText : ''} />
            </div>
          );
        })}
      </div>
    </motion.div>
  );
}
