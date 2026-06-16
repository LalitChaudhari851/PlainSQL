import React, { useState } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { Copy, Database, RefreshCw, Rows3, Sigma, ThumbsDown, ThumbsUp, Check, AlertTriangle } from 'lucide-react';
import PipelineTrace from '../pipeline/PipelineTrace';
import SQLBlock from '../artifacts/SQLBlock';
import ResultTable from '../artifacts/ResultTable';
import ChartView from '../artifacts/ChartView';
import MetaBadges from '../artifacts/MetaBadges';
import InsightBlock from '../artifacts/InsightBlock';
import useChatStore from '../../store/useChatStore';

const MarkdownText = React.memo(function MarkdownText({ text = '' }) {
  const html = String(text)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.*?)\*/g, '<em>$1</em>')
    .replace(/`([^`]+)`/g, '<code class="font-mono text-brand-light bg-white/10 px-1.5 py-0.5 rounded text-xs">$1</code>')
    .replace(/\n/g, '<br/>');
  return <span dangerouslySetInnerHTML={{ __html: html }} />;
});

function extractRows(data) {
  const rows = data?.answer || data?.data || [];
  return Array.isArray(rows) && rows.length && typeof rows[0] === 'object' ? rows : [];
}

function numericColumns(rows) {
  if (!rows.length) return [];
  return Object.keys(rows[0]).filter(col => rows.some(row => Number.isFinite(Number(row[col]))));
}

function formatNumber(value) {
  return Number(value || 0).toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function ResultSummary({ rows }) {
  if (!rows.length) return null;
  const nums = numericColumns(rows);
  const metric = nums[0];
  const total = metric ? rows.reduce((sum, row) => sum + Number(row[metric] || 0), 0) : rows.length;
  const cards = [
    { icon: Rows3, label: 'Rows returned', value: formatNumber(rows.length) },
    { icon: Sigma, label: metric ? `Total ${metric.replace(/_/g, ' ')}` : 'Records', value: formatNumber(total) },
    { icon: Database, label: 'Columns', value: Object.keys(rows[0]).length },
  ];

  return (
    <div className="mb-4 grid gap-3 sm:grid-cols-3">
      {cards.map(({ icon: Icon, label, value }, i) => (
        <motion.div
          key={label}
          initial={{ opacity: 0, y: 6 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: i * 0.05 }}
          className="rounded-xl p-3 bg-surface-1 border border-border-1"
        >
          <div className="mb-1 flex items-center gap-2 text-t4">
            <Icon size={12} />
            <span className="text-[10px] font-bold uppercase tracking-wider">{label}</span>
          </div>
          <p className="truncate font-mono text-lg font-bold text-white">{value}</p>
        </motion.div>
      ))}
    </div>
  );
}

function ThinkingStatus({ stage }) {
  return (
    <div className="mb-4 flex items-center gap-3 rounded-xl px-4 py-3 bg-surface-1 border border-border-1">
      <div className="flex gap-1">
        {[0, 1, 2].map(i => (
          <motion.div
            key={i}
            animate={{ opacity: [0.3, 1, 0.3] }}
            transition={{ duration: 1.2, repeat: Infinity, delay: i * 0.18 }}
            className="h-1.5 w-1.5 rounded-full bg-brand-light"
          />
        ))}
      </div>
      <span className="text-xs text-t3 font-medium">{stage || 'Planning retrieval strategy...'}</span>
    </div>
  );
}

function UserBubble({ content }) {
  return (
    <div className="flex justify-end w-full mb-6">
      <div
        className="max-w-[80%] rounded-2xl rounded-tr-sm px-4 py-3 text-sm leading-relaxed text-white shadow-md font-medium"
        style={{
          background: 'linear-gradient(135deg, var(--brand), rgba(99,102,241,0.7))',
          border: '1px solid rgba(255,255,255,0.08)',
          boxShadow: '0 4px 16px rgba(99,102,241,0.15)',
        }}
      >
        {content}
      </div>
    </div>
  );
}

function AssistantBubble({ message, chatId, onRegenerate }) {
  const addToast = useChatStore(s => s.addToast);
  const setFeedback = useChatStore(s => s.setFeedback);
  const [followUpOpen, setFollowUpOpen] = useState(false);
  const [copied, setCopied] = useState(false);

  const data = message.data ?? {};
  const rows = extractRows(data);
  const isChatMode = Boolean(message._chatMode);
  const hasSQL = Boolean(data.sql || message._streamingSql);
  const pipelineStep = (message.pending || message.streaming) ? (message._pipelineStep ?? 0) : 5;

  const handleFeedback = async (rating) => {
    setFeedback(chatId, message.id, rating);
    try {
      const { submitFeedback } = await import('../../api/client');
      await submitFeedback({ message_id: message.id, user_query: message._userQuery ?? '', generated_sql: data.sql ?? '', rating });
      addToast(rating === 'up' ? 'Feedback recorded' : 'Thanks, feedback saved', 'success');
    } catch {
      addToast('Could not save feedback', 'error');
    }
  };

  const handleCopyResponse = async () => {
    const parts = [message.streamText ?? data.message ?? '', data.sql ? `\n\nSQL:\n${data.sql}` : ''].filter(Boolean).join('');
    await navigator.clipboard.writeText(parts).catch(() => {});
    setCopied(true);
    addToast('Response copied', 'success');
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="mb-6 flex items-start gap-4">
      {/* Assistant avatar */}
      <div
        className="grid h-8 w-8 flex-shrink-0 place-items-center rounded-xl shadow-md border border-white/[0.05]"
        style={{
          background: 'linear-gradient(135deg, var(--brand), var(--brand-cyan))',
          boxShadow: '0 4px 12px rgba(99,102,241,0.25)',
        }}
      >
        <span className="text-xs font-black text-white leading-none">S</span>
      </div>

      <div className="min-w-0 flex-1">
        {/* Main card */}
        <div
          className="rounded-2xl rounded-tl-sm p-5 shadow-lg border border-border-1 relative overflow-hidden"
          style={{
            background: 'var(--surface-05)',
            boxShadow: '0 4px 24px rgba(0,0,0,0.12)',
          }}
        >
          {message.error ? (
            /* Error state with retry option */
            <div className="rounded-xl p-4 bg-red-500/5 border border-red-500/15 border-l-4 border-l-danger">
              <div className="flex items-start gap-3">
                <AlertTriangle size={16} className="text-danger flex-shrink-0 mt-0.5" />
                <div className="flex-1 min-w-0">
                  <p className="text-sm text-red-100/90 font-medium mb-3 leading-relaxed">{message.error}</p>
                  <button
                    onClick={onRegenerate}
                    className="text-xs font-semibold px-3 py-1.5 rounded-lg transition-all bg-danger/10 border border-danger/25 text-red-200 hover:bg-danger/20 hover:text-white"
                  >
                    <RefreshCw size={11} className="inline mr-1.5 animate-pulse" />
                    Try again
                  </button>
                </div>
              </div>
            </div>
          ) : (
            <>
              <PipelineTrace
                activeStep={message.streaming || message.pending ? pipelineStep : 5}
                isChatMode={isChatMode}
                stageText={message._stageText}
              />

              {message.pending && !message.data && !message.streamText && !message._streamingSql && !message._pipelineStep && (
                <ThinkingStatus stage={message._stageText} />
              )}

              {Boolean(data.sql) && (
                <MetaBadges intent={data.intent} executionTimeMs={data.execution_time_ms} rowCount={data.row_count ?? rows.length} />
              )}

              {hasSQL && (
                <div className={message._pipelineStep === 1 && message.streaming ? 'typing-cursor block' : ''}>
                  <SQLBlock sql={message._streamingSql || data.sql} messageId={message.id} />
                </div>
              )}

              {rows.length > 0 && <ResultSummary rows={rows} />}

              {(message.streamText || data.message) && (
                <div className={`mb-4 text-sm leading-relaxed text-t2 font-medium ${message.streaming && message._pipelineStep >= 4 ? 'typing-cursor block' : ''}`}>
                  <MarkdownText text={message.streamText || data.message} />
                </div>
              )}

              {rows.length > 0 && <ResultTable rows={rows} />}
              {rows.length >= 2 && <ChartView rows={rows} />}

              <InsightBlock insights={data.insights} explanation={data.explanation || data.sql_explanation} />

              {/* Follow-up questions */}
              {Array.isArray(data.follow_ups) && data.follow_ups.length > 0 && !message.streaming && (
                <div className="mt-4 pt-3 border-t border-border-1">
                  <button
                    onClick={() => setFollowUpOpen(v => !v)}
                    className="text-[11px] font-bold uppercase tracking-wider text-brand-light hover:text-white transition-colors flex items-center gap-1"
                  >
                    <span>{followUpOpen ? 'Hide' : 'Show'} follow-up questions</span>
                    <span className="text-t4 font-mono font-normal">({data.follow_ups.length})</span>
                  </button>
                  <AnimatePresence>
                    {followUpOpen && (
                      <motion.div
                        initial={{ height: 0, opacity: 0 }}
                        animate={{ height: 'auto', opacity: 1 }}
                        exit={{ height: 0, opacity: 0 }}
                        className="mt-2.5 flex flex-wrap gap-2 overflow-hidden"
                      >
                        {data.follow_ups.slice(0, 4).map((q, i) => (
                          <button
                            key={i}
                            onClick={() => window.dispatchEvent(new CustomEvent('plainsql:submit', { detail: { query: q } }))}
                            className="rounded-lg px-3 py-1.5 text-xs text-t2 border border-border-1 bg-surface-1 hover:bg-surface-hover hover:border-brand/35 hover:text-white transition-all font-medium text-left"
                          >
                            {q}
                          </button>
                        ))}
                      </motion.div>
                    )}
                  </AnimatePresence>
                </div>
              )}
            </>
          )}
        </div>

        {/* Action bar */}
        {!message.pending && !message.streaming && !message.error && (
          <div className="mt-2 flex items-center justify-between pl-1">
            {/* Left group: Copy & Retry */}
            <div className="flex items-center gap-1">
              <button
                onClick={handleCopyResponse}
                className="flex items-center gap-1 rounded-lg px-2.5 py-1.5 text-xs font-semibold text-t4 hover:bg-white/[0.04] hover:text-t2 transition-all"
                aria-label="Copy response"
              >
                {copied ? <Check size={12} className="text-success" /> : <Copy size={12} />}
                <span>{copied ? 'Copied' : 'Copy'}</span>
              </button>
              <button
                onClick={onRegenerate}
                className="flex items-center gap-1 rounded-lg px-2.5 py-1.5 text-xs font-semibold text-t4 hover:bg-white/[0.04] hover:text-t2 transition-all"
                aria-label="Regenerate response"
              >
                <RefreshCw size={12} />
                <span>Retry</span>
              </button>
            </div>

            {/* Right group: Feedback */}
            <div className="flex items-center gap-0.5 rounded-lg p-0.5 bg-surface-1 border border-border-1 shadow-inner">
              <button
                onClick={() => handleFeedback('up')}
                className={`rounded-md p-1.5 transition-all ${
                  message._feedback === 'up'
                    ? 'bg-success/15 text-success'
                    : 'text-t4 hover:bg-white/[0.04] hover:text-t2'
                }`}
                aria-label="Thumbs up"
              >
                <ThumbsUp size={12} />
              </button>
              <button
                onClick={() => handleFeedback('down')}
                className={`rounded-md p-1.5 transition-all ${
                  message._feedback === 'down'
                    ? 'bg-danger/15 text-danger'
                    : 'text-t4 hover:bg-white/[0.04] hover:text-t2'
                }`}
                aria-label="Thumbs down"
              >
                <ThumbsDown size={12} />
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default function MessageBubble({ message, chatId, onRegenerate }) {
  if (message.role === 'user') return <UserBubble content={message.content} />;
  return <AssistantBubble message={message} chatId={chatId} onRegenerate={onRegenerate} />;
}
