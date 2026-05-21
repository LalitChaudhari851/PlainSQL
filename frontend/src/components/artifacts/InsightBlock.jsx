import { motion } from 'framer-motion';
import { Lightbulb, TrendingUp } from 'lucide-react';

function stripMarkdown(text) {
  return String(text)
    .replace(/\*\*(.*?)\*\*/g, '$1')
    .replace(/\*(.*?)\*/g, '$1')
    .replace(/`([^`]+)`/g, '$1');
}

export default function InsightBlock({ insights, explanation }) {
  if (!insights?.length && !explanation) return null;

  return (
    <div className="space-y-3 mb-3">
      {explanation && (
        <motion.div
          initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }}
          className="rounded-xl p-4"
          style={{ background: 'rgba(167,139,250,0.06)', border: '1px solid rgba(167,139,250,0.15)' }}>
          <div className="flex items-center gap-2 mb-2">
            <TrendingUp size={13} className="text-violet-400" />
            <span className="text-xs font-semibold text-violet-400 uppercase tracking-wide">SQL Reasoning</span>
          </div>
          <p className="text-sm text-white/65 leading-relaxed">{stripMarkdown(explanation)}</p>
        </motion.div>
      )}

      {insights?.length > 0 && (
        <motion.div
          initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }}
          className="rounded-xl p-4"
          style={{ background: 'rgba(245,158,11,0.06)', border: '1px solid rgba(245,158,11,0.15)' }}>
          <div className="flex items-center gap-2 mb-2">
            <Lightbulb size={13} className="text-amber-400" />
            <span className="text-xs font-semibold text-amber-400 uppercase tracking-wide">AI Insights</span>
          </div>
          <ul className="space-y-1.5">
            {insights.map((item, i) => (
              <li key={i} className="flex items-start gap-2 text-sm text-white/65">
                <span className="text-amber-500 mt-0.5 flex-shrink-0">•</span>
                {stripMarkdown(String(item))}
              </li>
            ))}
          </ul>
        </motion.div>
      )}
    </div>
  );
}
