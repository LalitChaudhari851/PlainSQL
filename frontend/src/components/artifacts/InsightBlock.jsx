import { useState } from 'react';
import { motion } from 'framer-motion';
import { Lightbulb, TrendingUp, ChevronDown } from 'lucide-react';

function stripMarkdown(text) {
  return String(text)
    .replace(/\*\*(.*?)\*\*/g, '$1')
    .replace(/\*(.*?)\*/g, '$1')
    .replace(/`([^`]+)`/g, '$1');
}

export default function InsightBlock({ insights, explanation }) {
  const [expanded, setExpanded] = useState(true);
  const showToggle = insights?.length > 3;

  if (!insights?.length && !explanation) return null;

  return (
    <div className="space-y-3 mb-4">
      {/* SQL Reasoning */}
      {explanation && (
        <motion.div
          initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }}
          className="rounded-xl p-4 relative overflow-hidden"
          style={{
            background: 'rgba(167,139,250,0.05)',
            border: '1px solid rgba(167,139,250,0.12)',
            borderLeft: '3px solid var(--brand-violet)',
          }}
        >
          <div className="flex items-center gap-2 mb-2">
            <TrendingUp size={13} style={{ color: 'var(--brand-violet)' }} />
            <span className="text-xs font-bold uppercase tracking-wider" style={{ color: 'var(--brand-violet)' }}>SQL Compilation Reasoning</span>
          </div>
          <p className="text-sm text-t2 leading-relaxed font-medium">{stripMarkdown(explanation)}</p>
        </motion.div>
      )}

      {/* AI Insights */}
      {insights?.length > 0 && (
        <motion.div
          initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }}
          className="rounded-xl p-4 relative overflow-hidden"
          style={{
            background: 'rgba(251,191,36,0.03)',
            border: '1px solid rgba(251,191,36,0.10)',
            borderLeft: '3px solid var(--warning)',
          }}
        >
          <div className="flex items-center justify-between mb-3.5">
            <div className="flex items-center gap-2">
              <Lightbulb size={13} style={{ color: 'var(--warning)' }} />
              <span className="text-xs font-bold uppercase tracking-wider" style={{ color: 'var(--warning)' }}>Executive Insights</span>
              <span className="text-[10px] text-t4 font-mono font-bold bg-white/[0.03] border border-white/[0.04] px-1.5 py-0.2 rounded-sm">{insights.length}</span>
            </div>
            {showToggle && (
              <button
                onClick={() => setExpanded(v => !v)}
                className="text-xs font-semibold text-t4 hover:text-t2 transition-colors flex items-center gap-1 focus-ring"
              >
                {expanded ? 'Show less' : 'Show all'}
                <ChevronDown size={11} className={`transition-transform ${expanded ? 'rotate-180' : ''}`} />
              </button>
            )}
          </div>
          <ul className="space-y-2.5">
            {(expanded ? insights : insights.slice(0, 3)).map((item, i) => (
              <motion.li
                key={i}
                initial={{ opacity: 0, x: -8 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: i * 0.05 }}
                className="flex items-start gap-3 text-sm text-t2 font-medium"
              >
                <span
                  className="flex-shrink-0 w-5 h-5 rounded-md flex items-center justify-center text-[10px] font-extrabold mt-0.5"
                  style={{
                    background: 'rgba(251,191,36,0.10)',
                    color: 'var(--warning)',
                    border: '1px solid rgba(251,191,36,0.18)',
                  }}
                >
                  {i + 1}
                </span>
                <span className="leading-relaxed">{stripMarkdown(String(item))}</span>
              </motion.li>
            ))}
          </ul>
        </motion.div>
      )}
    </div>
  );
}
