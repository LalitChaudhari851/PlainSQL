import { motion } from 'framer-motion';
import { BarChart3, DatabaseZap, GitBranch, ShieldCheck, Sparkles, Workflow } from 'lucide-react';

const PROMPTS = [
  {
    icon: BarChart3,
    title: 'Expansion revenue',
    query: 'Show net revenue retention by customer segment for the last 4 quarters',
  },
  {
    icon: Workflow,
    title: 'Pipeline quality',
    query: 'Which opportunities have high ARR but stalled for more than 30 days?',
  },
  {
    icon: GitBranch,
    title: 'Support impact',
    query: 'Compare churn risk for customers with critical tickets versus healthy accounts',
  },
  {
    icon: DatabaseZap,
    title: 'Product usage',
    query: 'Rank workspaces by query volume, failed executions, and active users this month',
  },
];

const CAPABILITIES = [
  { label: 'Retrieval', value: 'Intelligent context' },
  { label: 'Routing', value: 'Smart analysis' },
  { label: 'Guardrails', value: 'Validated SQL' },
  { label: 'Traceability', value: 'Full transparency' },
];

const DATASET_STATS = [
  ['18', 'tables'],
  ['27K+', 'demo rows'],
  ['5', 'domains'],
  ['36 mo', 'history'],
];

export default function WelcomeScreen({ onPrompt }) {
  return (
    <div className="relative flex min-h-full flex-col justify-center px-4 py-8">

      <div className="relative mx-auto grid w-full max-w-5xl gap-5 lg:grid-cols-[1.05fr_0.95fr] lg:items-center">
        <section className="space-y-6">
          <motion.div
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            className="inline-flex items-center gap-2 rounded-full border border-cyan-400/20 bg-cyan-400/10 px-3 py-1 text-xs font-medium text-cyan-200"
          >
            <Sparkles size={13} />
            AI data copilot for production SQL workflows
          </motion.div>

          <div>
            <motion.h2
              initial={{ opacity: 0, y: 14 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.04 }}
              className="max-w-2xl text-left text-3xl font-semibold leading-tight text-white md:text-4xl"
            >
              Ask the business question. PlainSQL shows the reasoning path.
            </motion.h2>
            <motion.p
              initial={{ opacity: 0, y: 14 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.08 }}
              className="mt-4 max-w-xl text-left text-sm leading-7 text-white/52"
            >
              Schema-aware retrieval, SQL generation, validation, execution, and insight synthesis are visible as a live pipeline, not hidden behind a spinner.
            </motion.p>
          </div>

          <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
            {CAPABILITIES.map((item, i) => (
              <motion.div
                key={item.label}
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.12 + i * 0.04 }}
                className="rounded-xl border border-white/[0.07] bg-white/[0.035] p-3"
              >
                <p className="text-[11px] font-semibold uppercase tracking-wide text-white/28">{item.label}</p>
                <p className="mt-1 text-xs text-white/70">{item.value}</p>
              </motion.div>
            ))}
          </div>
        </section>

        <section className="rounded-2xl border border-white/[0.08] bg-white/[0.035] p-4 shadow-2xl shadow-black/30 backdrop-blur-xl">
          <div className="mb-4 flex items-center justify-between">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-white/30">Demo Workspace</p>
              <h3 className="mt-1 text-lg font-semibold text-white">SaaS revenue intelligence</h3>
            </div>
            <div className="rounded-xl border border-emerald-400/20 bg-emerald-400/10 px-3 py-1 text-xs font-medium text-emerald-300">
              Indexed
            </div>
          </div>

          <div className="mb-4 grid grid-cols-4 gap-2">
            {DATASET_STATS.map(([value, label]) => (
              <div key={label} className="rounded-xl border border-white/[0.06] bg-black/20 p-3">
                <p className="font-mono text-base font-semibold text-white">{value}</p>
                <p className="text-[11px] uppercase tracking-wide text-white/28">{label}</p>
              </div>
            ))}
          </div>

          <div className="grid gap-2">
            {PROMPTS.map(({ icon: Icon, title, query }, i) => (
              <motion.button
                key={query}
                initial={{ opacity: 0, x: 14 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: 0.18 + i * 0.05, duration: 0.28 }}
                whileHover={{ x: 4 }}
                whileTap={{ scale: 0.99 }}
                onClick={() => onPrompt(query)}
                className="group rounded-xl border border-white/[0.07] bg-white/[0.03] p-4 text-left transition-all hover:border-cyan-400/30 hover:bg-cyan-400/[0.06] focus-ring"
              >
                <div className="flex items-start gap-3">
                  <div className="mt-0.5 rounded-lg border border-white/[0.08] bg-white/[0.05] p-2 text-cyan-300 transition-colors group-hover:text-white">
                    <Icon size={15} />
                  </div>
                  <div className="min-w-0">
                    <p className="text-sm font-semibold text-white">{title}</p>
                    <p className="mt-1 text-xs leading-5 text-white/42">{query}</p>
                  </div>
                </div>
              </motion.button>
            ))}
          </div>

          <div className="mt-4 flex items-center gap-2 rounded-xl border border-violet-400/15 bg-violet-400/[0.06] px-3 py-2 text-xs text-violet-200/80">
            <ShieldCheck size={14} />
            Every query is validated against read-only execution guardrails.
          </div>
        </section>
      </div>
    </div>
  );
}
