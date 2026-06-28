import { useEffect, useRef, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  MessageSquare,
  Columns3,
  Code2,
  Link,
  GitMerge,
  AlertTriangle,
  FileText,
} from 'lucide-react';

const menuVariants = {
  hidden: { opacity: 0, y: -4, scale: 0.95 },
  visible: {
    opacity: 1,
    y: 0,
    scale: 1,
    transition: { duration: 0.14, ease: [0.16, 1, 0.3, 1], staggerChildren: 0.02 },
  },
  exit: { opacity: 0, y: -2, transition: { duration: 0.1 } },
};

const itemVariants = {
  hidden: { opacity: 0, x: -4 },
  visible: { opacity: 1, x: 0 },
};

const AI_ACTIONS = [
  { icon: MessageSquare, label: 'Describe this table', query: (t) => `Describe the ${t} table in detail` },
  { icon: Columns3, label: 'Explain columns', query: (t) => `Explain all columns in the ${t} table` },
  { icon: Code2, label: 'Generate SELECT query', query: (t) => `Generate a useful SELECT query for ${t}` },
  { icon: Link, label: 'Find relationships', query: (t) => `Show all relationships for the ${t} table` },
  { icon: GitMerge, label: 'Suggest joins', query: (t) => `Suggest useful JOIN queries involving ${t}` },
  { icon: AlertTriangle, label: 'Find anomalies', query: (t) => `Find potential data anomalies in ${t}` },
  { icon: FileText, label: 'Generate docs', query: (t) => `Generate documentation for the ${t} table` },
];

/**
 * AIActionsMenu — dropdown with AI-powered quick actions for a table.
 * Each action dispatches a natural language query into the existing chat pipeline.
 */
export default function AIActionsMenu({ tableName, onClose, anchorRef }) {
  const menuRef = useRef(null);

  // Close on click-outside
  useEffect(() => {
    const handler = (e) => {
      if (
        menuRef.current && !menuRef.current.contains(e.target) &&
        anchorRef?.current && !anchorRef.current.contains(e.target)
      ) {
        onClose();
      }
    };
    document.addEventListener('mousedown', handler, true);
    return () => document.removeEventListener('mousedown', handler, true);
  }, [onClose, anchorRef]);

  // Close on Escape
  useEffect(() => {
    const handler = (e) => {
      if (e.key === 'Escape') { e.preventDefault(); onClose(); }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  const handleAction = useCallback((queryFn) => {
    window.dispatchEvent(new CustomEvent('plainsql:submit', {
      detail: { query: queryFn(tableName) }
    }));
    onClose();
  }, [tableName, onClose]);

  return (
    <AnimatePresence>
      <motion.div
        ref={menuRef}
        variants={menuVariants}
        initial="hidden"
        animate="visible"
        exit="exit"
        className="ai-actions-menu absolute right-0 top-full mt-1 z-50 min-w-[200px] py-1.5 rounded-lg shadow-2xl"
        role="menu"
        aria-label={`AI actions for ${tableName}`}
      >
        {/* Header */}
        <div className="px-3 py-1.5 mb-0.5">
          <span className="text-[9px] font-bold text-t4 uppercase tracking-wider">
            AI Actions
          </span>
        </div>

        {AI_ACTIONS.map((action, i) => (
          <motion.button
            key={i}
            variants={itemVariants}
            onClick={() => handleAction(action.query)}
            className="ai-action-item w-full flex items-center gap-2.5 px-3 py-[6px] text-xs text-left text-t2 hover:text-white hover:bg-brand-dim/30 transition-colors rounded-sm"
            role="menuitem"
          >
            <action.icon size={13} className="text-brand-light/70 flex-shrink-0" />
            {action.label}
          </motion.button>
        ))}
      </motion.div>
    </AnimatePresence>
  );
}
