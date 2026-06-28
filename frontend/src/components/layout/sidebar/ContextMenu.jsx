import { useEffect, useRef, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { motion, AnimatePresence } from 'framer-motion';
import { Copy, Table2, Code2, Sparkles, Columns3 } from 'lucide-react';

const menuVariants = {
  hidden: { opacity: 0, scale: 0.92, y: -4 },
  visible: {
    opacity: 1,
    scale: 1,
    y: 0,
    transition: { duration: 0.12, ease: [0.16, 1, 0.3, 1] },
  },
  exit: {
    opacity: 0,
    scale: 0.95,
    transition: { duration: 0.08 },
  },
};

/**
 * ContextMenu — portal-rendered right-click menu for tables.
 * Positioned at cursor coordinates, closes on click-outside or Escape.
 */
export default function ContextMenu({
  tableName,
  position,
  onClose,
  onExpandColumns,
  onAskAI,
}) {
  const menuRef = useRef(null);

  // Close on click-outside
  useEffect(() => {
    const handler = (e) => {
      if (menuRef.current && !menuRef.current.contains(e.target)) {
        onClose();
      }
    };
    document.addEventListener('mousedown', handler, true);
    return () => document.removeEventListener('mousedown', handler, true);
  }, [onClose]);

  // Close on Escape
  useEffect(() => {
    const handler = (e) => {
      if (e.key === 'Escape') { e.preventDefault(); onClose(); }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  const copyToClipboard = useCallback(async (text) => {
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      // Fallback for non-secure contexts
      const textarea = document.createElement('textarea');
      textarea.value = text;
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand('copy');
      document.body.removeChild(textarea);
    }
    onClose();
  }, [onClose]);

  const handleGenerateSQL = useCallback(() => {
    window.dispatchEvent(new CustomEvent('plainsql:submit', {
      detail: { query: `Generate a useful SQL query for the ${tableName} table` }
    }));
    onClose();
  }, [tableName, onClose]);

  const items = [
    {
      icon: Copy,
      label: 'Copy table name',
      action: () => copyToClipboard(tableName),
    },
    {
      icon: Code2,
      label: 'Copy SELECT *',
      action: () => copyToClipboard(`SELECT * FROM ${tableName} LIMIT 100;`),
    },
    { divider: true },
    {
      icon: Columns3,
      label: 'View columns',
      action: () => { onExpandColumns?.(); onClose(); },
    },
    {
      icon: Table2,
      label: 'Generate SQL',
      action: handleGenerateSQL,
    },
    { divider: true },
    {
      icon: Sparkles,
      label: 'Ask AI...',
      action: () => { onAskAI?.(); onClose(); },
      accent: true,
    },
  ];

  // Clamp position to viewport
  const x = Math.min(position.x, window.innerWidth - 200);
  const y = Math.min(position.y, window.innerHeight - 280);

  return createPortal(
    <AnimatePresence>
      <motion.div
        ref={menuRef}
        variants={menuVariants}
        initial="hidden"
        animate="visible"
        exit="exit"
        className="context-menu fixed z-[9999] min-w-[180px] py-1.5 rounded-lg shadow-2xl"
        style={{ left: x, top: y }}
        role="menu"
        aria-label={`Actions for ${tableName}`}
      >
        {items.map((item, i) =>
          item.divider ? (
            <div key={i} className="h-px bg-white/[0.06] my-1 mx-2" />
          ) : (
            <button
              key={i}
              onClick={item.action}
              className={`context-menu-item w-full flex items-center gap-2.5 px-3 py-[6px] text-xs text-left transition-colors ${
                item.accent
                  ? 'text-brand-light hover:bg-brand-dim/40'
                  : 'text-t2 hover:bg-white/[0.05]'
              }`}
              role="menuitem"
            >
              <item.icon size={13} className={item.accent ? 'text-brand-light' : 'text-t4'} />
              {item.label}
            </button>
          )
        )}
      </motion.div>
    </AnimatePresence>,
    document.body
  );
}
