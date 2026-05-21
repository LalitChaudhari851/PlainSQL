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
        className="grid h-5 w-5 place-items-center rounded-md transition-colors"
        style={{
          background: isDone ? 'rgba(34,197,94,0.12)' : isActive ? 'rgba(59,130,246,0.15)' : 'rgba(255,255,255,0.04)',
          border: `1px solid ${isDone ? 'rgba(34,197,94,0.25)' : isActive ? 'rgba(59,130,246,0.3)' : 'rgba(255,255,255,0.06)'}`,
        }}
      >
        {isDone ? (
          <CheckCircle2 size={11} className="text-emerald-400" />
        ) : isActive ? (
          <motion.div animate={{ rotate: 360 }} transition={{ duration: 1, repeat: Infinity, ease: 'linear' }}>
            <Loader2 size={11} className="text-blue-400" />
          </motion.div>
        ) : (
          <Icon size={11} style={{ color: 'rgba(255,255,255,0.25)' }} />
        )}
      </div>
      <span className={`text-[11px] font-medium transition-colors ${
        isDone ? 'text-emerald-300/70' : isActive ? 'text-blue-300' : 'text-white/25'
      }`}>
        {isActive && stageText ? stageText : step.label}
      </span>
    </div>
  );
}

function Connector({ isDone }) {
  return (
    <div
      className="hidden sm:block h-px flex-1 min-w-3 max-w-6 mx-0.5"
      style={{ background: isDone ? 'rgba(34,197,94,0.2)' : 'rgba(255,255,255,0.06)' }}
    />
  );
}

export default function PipelineTrace({ activeStep = -1, isChatMode = false, stageText = '' }) {
  if (isChatMode) {
    return (
      <div className="mb-3 inline-flex items-center gap-1.5 rounded-lg bg-cyan-400/[0.06] px-2.5 py-1 border border-cyan-400/15">
        <Sparkles size={11} className="text-cyan-300" />
        <span className="text-[11px] font-medium text-cyan-200/70">Conversational</span>
      </div>
    );
  }

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      className="mb-3 flex flex-wrap items-center gap-y-1.5 rounded-xl border border-white/[0.06] bg-white/[0.02] px-3 py-2"
    >
      {STEPS.map((step, i) => {
        const isDone = i < activeStep || activeStep >= STEPS.length;
        const isActive = i === activeStep && activeStep < STEPS.length;

        return (
          <div key={step.key} className="contents">
            <StepDot step={step} isDone={isDone} isActive={isActive} stageText={isActive ? stageText : ''} />
            {i < STEPS.length - 1 && <Connector isDone={i < activeStep} />}
          </div>
        );
      })}
    </motion.div>
  );
}
