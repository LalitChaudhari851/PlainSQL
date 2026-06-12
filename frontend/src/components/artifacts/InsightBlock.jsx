import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
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
            borderLeft: '3px solid rgba(167,139,250,0.4)',
          }}
        >
          <div className="flex items-center gap-2 mb-2">
            <TrendingUp size={13} className="text-violet-400" />
            <span className="text-xs font-semibold text-violet-400 uppercase tracking-wide">SQL Reasoning</span>
          </div>
          <p className="text-sm text-t2 leading-relaxed">{stripMarkdown(explanation)}</p>
        </motion.div>
      )}

      {/* AI Insights */}
      {insights?.length > 0 && (
        <motion.div
          initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }}
          className="rounded-xl p-4 relative overflow-hidden"
          style={{
            background: 'rgba(245,158,11,0.04)',
            border: '1px solid rgba(245,158,11,0.12)',
            borderLeft: '3px solid rgba(245,158,11,0.4)',
          }}
        >
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <Lightbulb size={13} className="text-amber-400" />
              <span className="text-xs font-semibold text-amber-400 uppercase tracking-wide">AI Insights</span>
              <span className="text-xs text-t4 font-mono">{insights.length}</span>
            </div>
            {showToggle && (
              <button
                onClick={() => setExpanded(v => !v)}
                className="text-xs text-t4 hover:text-t2 transition-colors flex items-center gap-1"
              >
                {expanded ? 'Show less' : 'Show all'}
                <ChevronDown size={11} className={`transition-transform ${expanded ? 'rotate-180' : ''}`} />
              </button>
            )}
          </div>
          <ul className="space-y-2">
            {(expanded ? insights : insights.slice(0, 3)).map((item, i) => (
              <motion.li
                key={i}
                initial={{ opacity: 0, x: -8 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: i * 0.05 }}
                className="flex items-start gap-3 text-sm text-t2"
              >
                <span
                  className="flex-shrink-0 w-5 h-5 rounded-md flex items-center justify-center text-[10px] font-bold mt-0.5"
                  style={{
                    background: 'rgba(245,158,11,0.12)',
                    color: '#fbbf24',
                    border: '1px solid rgba(245,158,11,0.2)',
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
