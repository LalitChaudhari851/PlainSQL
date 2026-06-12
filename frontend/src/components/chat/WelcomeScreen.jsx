import { motion } from 'framer-motion';
import { BarChart3, DatabaseZap, GitBranch, ShieldCheck, Sparkles, Workflow, ArrowRight } from 'lucide-react';

const PROMPTS = [
  {
    icon: BarChart3,
    title: 'Expansion revenue',
    query: 'Show net revenue retention by customer segment for the last 4 quarters',
    color: '#3b82f6',
  },
  {
    icon: Workflow,
    title: 'Pipeline quality',
    query: 'Which opportunities have high ARR but stalled for more than 30 days?',
    color: '#06b6d4',
  },
  {
    icon: GitBranch,
    title: 'Support impact',
    query: 'Compare churn risk for customers with critical tickets versus healthy accounts',
    color: '#a78bfa',
  },
  {
    icon: DatabaseZap,
    title: 'Product usage',
    query: 'Rank workspaces by query volume, failed executions, and active users this month',
    color: '#22c55e',
  },
];

const CAPABILITIES = [
  { icon: '🔍', label: 'Hybrid RAG', desc: 'Vector + BM25 retrieval combined' },
  { icon: '⚡', label: 'Smart routing', desc: 'Picks the best LLM for the task' },
  { icon: '🛡️', label: 'SQL guardrails', desc: 'Read-only, validated before execution' },
  { icon: '📊', label: 'Full transparency', desc: 'See every step of the pipeline' },
];

export default function WelcomeScreen({ onPrompt }) {
  return (
    <div className="relative flex min-h-full flex-col items-center justify-center px-4 py-10">

      {/* Hero section — single centered column */}
      <div className="w-full max-w-2xl mx-auto text-center mb-8">
        <motion.div
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          className="inline-flex items-center gap-2 rounded-full px-3.5 py-1.5 text-xs font-medium mb-6"
          style={{
            background: 'rgba(6,182,212,0.08)',
            border: '1px solid rgba(6,182,212,0.2)',
            color: '#67e8f9',
          }}
        >
          <Sparkles size={13} />
          AI data copilot for production SQL workflows
        </motion.div>

        <motion.h2
          initial={{ opacity: 0, y: 14 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.04 }}
          className="text-3xl md:text-4xl font-semibold leading-tight text-white mb-4"
        >
          Ask the business question.
          <br />
          <span className="text-gradient">PlainSQL shows the reasoning.</span>
        </motion.h2>

        <motion.p
          initial={{ opacity: 0, y: 14 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.08 }}
          className="text-t3 text-sm leading-7 max-w-lg mx-auto"
        >
          Schema retrieval, SQL generation, validation, execution, and insight
          synthesis — visible as a live pipeline, not hidden behind a spinner.
        </motion.p>
      </div>

      {/* Capabilities row */}
      <motion.div
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.12 }}
        className="w-full max-w-2xl mx-auto grid grid-cols-2 sm:grid-cols-4 gap-2 mb-8"
      >
        {CAPABILITIES.map((item, i) => (
          <motion.div
            key={item.label}
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.14 + i * 0.04 }}
            className="rounded-xl p-3 text-center"
            style={{
              background: 'var(--surface-1)',
              border: '1px solid var(--border-1)',
            }}
          >
            <span className="text-base mb-1 block">{item.icon}</span>
            <p className="text-xs font-semibold text-t2 mb-0.5">{item.label}</p>
            <p className="text-[11px] text-t4 leading-snug hidden sm:block">{item.desc}</p>
          </motion.div>
        ))}
      </motion.div>

      {/* Prompt cards */}
      <div className="w-full max-w-2xl mx-auto">
        <motion.p
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.2 }}
          className="text-xs font-semibold uppercase tracking-widest text-t4 mb-3 px-1"
        >
          Try a sample query
        </motion.p>

        <div className="grid gap-2 sm:grid-cols-2">
          {PROMPTS.map(({ icon: Icon, title, query, color }, i) => (
            <motion.button
              key={query}
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.22 + i * 0.05, duration: 0.3 }}
              whileHover={{ y: -2, boxShadow: `0 8px 30px ${color}15` }}
              whileTap={{ scale: 0.99 }}
              onClick={() => onPrompt(query)}
              className="group rounded-xl p-4 text-left transition-all focus-ring"
              style={{
                background: 'var(--surface-1)',
                border: '1px solid var(--border-1)',
              }}
              onMouseEnter={e => {
                e.currentTarget.style.borderColor = `${color}40`;
                e.currentTarget.style.background = `${color}08`;
              }}
              onMouseLeave={e => {
                e.currentTarget.style.borderColor = 'var(--border-1)';
                e.currentTarget.style.background = 'var(--surface-1)';
              }}
            >
              <div className="flex items-start gap-3">
                <div
                  className="mt-0.5 rounded-lg p-2 flex-shrink-0 transition-colors"
                  style={{
                    background: `${color}12`,
                    border: `1px solid ${color}20`,
                  }}
                >
                  <Icon size={15} style={{ color }} />
                </div>
                <div className="min-w-0 flex-1">
                  <p className="text-sm font-semibold text-white mb-1 flex items-center gap-1.5">
                    {title}
                    <ArrowRight size={12} className="text-t4 group-hover:text-t2 group-hover:translate-x-0.5 transition-all opacity-0 group-hover:opacity-100" />
                  </p>
                  <p className="text-xs leading-5 text-t3">{query}</p>
                </div>
              </div>
            </motion.button>
          ))}
        </div>

        {/* Security note */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.5 }}
          className="mt-4 flex items-center justify-center gap-2 py-2 text-xs text-t4"
        >
          <ShieldCheck size={13} className="text-emerald-400/60" />
          Every query is validated against read-only execution guardrails
        </motion.div>
      </div>
    </div>
  );
}
