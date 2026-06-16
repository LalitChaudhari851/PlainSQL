import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Copy, Bookmark, ChevronDown, ChevronRight, Code2, Check } from 'lucide-react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { oneDark } from 'react-syntax-highlighter/dist/esm/styles/prism';
import useChatStore from '../../store/useChatStore';

const customStyle = {
  ...oneDark,
  'pre[class*="language-"]': {
    ...oneDark['pre[class*="language-"]'],
    background: 'transparent',
    padding: '0',
    margin: '0',
    fontSize: '13px',
    lineHeight: '1.7',
  },
  'code[class*="language-"]': {
    ...oneDark['code[class*="language-"]'],
    background: 'transparent',
    fontSize: '13px',
  },
};

export default function SQLBlock({ sql, messageId }) {
  const addToast = useChatStore(s => s.addToast);
  const saveSqlQuery = useChatStore(s => s.saveSqlQuery);
  const [collapsed, setCollapsed] = useState(false);
  const [copied, setCopied] = useState(false);
  const [saved, setSaved] = useState(false);

  if (!sql) return null;

  const lineCount = sql.split('\n').length;

  const handleCopy = async () => {
    await navigator.clipboard.writeText(sql).catch(() => {});
    setCopied(true);
    addToast('SQL copied to clipboard', 'success');
    setTimeout(() => setCopied(false), 2000);
  };

  const handleSave = () => {
    saveSqlQuery(sql);
    setSaved(true);
    addToast('Query saved to library', 'success');
    setTimeout(() => setSaved(false), 2000);
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      className="rounded-xl overflow-hidden mb-4"
      style={{
        border: '1px solid rgba(99,102,241,0.18)',
        background: 'rgba(99,102,241,0.03)',
      }}
    >
      {/* Header */}
      <div
        className="flex items-center justify-between px-4 py-2.5"
        style={{
          background: 'rgba(99,102,241,0.06)',
          borderBottom: '1px solid rgba(99,102,241,0.10)',
        }}
      >
        <div className="flex items-center gap-2">
          <Code2 size={13} className="text-brand-light" />
          <span className="text-xs font-bold text-brand-light">Generated SQL</span>
          <span className="text-[10px] text-t4 font-mono font-medium bg-white/[0.03] border border-white/[0.04] px-1 rounded tabular-nums">{lineCount} {lineCount === 1 ? 'line' : 'lines'}</span>
        </div>
        <div className="flex items-center gap-1">
          <button
            onClick={handleSave}
            className="flex items-center gap-1 px-2.5 py-1 rounded-lg text-xs font-semibold text-t3 hover:text-white hover:bg-white/10 transition-all"
            aria-label="Save query"
          >
            {saved ? <Check size={11} className="text-success" /> : <Bookmark size={11} />}
            <span className="hidden sm:inline">{saved ? 'Saved' : 'Save'}</span>
          </button>
          <button
            onClick={handleCopy}
            className="flex items-center gap-1 px-2.5 py-1 rounded-lg text-xs font-semibold text-t3 hover:text-white hover:bg-white/10 transition-all"
            aria-label="Copy SQL"
          >
            {copied ? <Check size={11} className="text-success" /> : <Copy size={11} />}
            <span className="hidden sm:inline">{copied ? 'Copied' : 'Copy'}</span>
          </button>
          <button
            onClick={() => setCollapsed(v => !v)}
            className="p-1 rounded-lg text-t4 hover:text-white hover:bg-white/10 transition-all"
            aria-label={collapsed ? 'Expand SQL' : 'Collapse SQL'}
          >
            {collapsed ? <ChevronRight size={13} /> : <ChevronDown size={13} />}
          </button>
        </div>
      </div>

      {/* Code with line numbers */}
      <AnimatePresence initial={false}>
        {!collapsed && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden"
          >
            <div className="px-4 py-3.5 overflow-x-auto font-mono bg-white/[0.015]">
              <SyntaxHighlighter
                language="sql"
                style={customStyle}
                customStyle={{ background: 'transparent', padding: 0, margin: 0 }}
                wrapLongLines={false}
                showLineNumbers={lineCount > 3}
                lineNumberStyle={{
                  color: 'rgba(255,255,255,0.12)',
                  fontSize: '11px',
                  paddingRight: '12px',
                  minWidth: '2em',
                  userSelect: 'none',
                }}
              >
                {sql}
              </SyntaxHighlighter>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
