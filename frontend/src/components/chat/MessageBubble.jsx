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
    .replace(/`([^`]+)`/g, '<code class="font-mono text-cyan-300 bg-white/10 px-1.5 py-0.5 rounded text-xs">$1</code>')
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
    <div className="mb-4 grid gap-2 sm:grid-cols-3">
      {cards.map(({ icon: Icon, label, value }, i) => (
        <motion.div
          key={label}
          initial={{ opacity: 0, y: 6 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: i * 0.05 }}
          className="rounded-xl p-3"
          style={{
            background: 'var(--surface-1)',
            border: '1px solid var(--border-1)',
          }}
        >
          <div className="mb-1.5 flex items-center gap-2 text-t4">
            <Icon size={12} />
            <span className="text-[11px] font-semibold uppercase tracking-wide">{label}</span>
          </div>
          <p className="truncate font-mono text-lg font-semibold text-white">{value}</p>
        </motion.div>
      ))}
    </div>
  );
}

function ThinkingStatus({ stage }) {
  return (
    <div className="mb-4 flex items-center gap-2.5 rounded-xl px-4 py-3"
      style={{
        background: 'var(--surface-1)',
        border: '1px solid var(--border-1)',
      }}
    >
      <div className="flex gap-1">
        {[0, 1, 2].map(i => (
          <motion.div
            key={i}
            animate={{ opacity: [0.3, 1, 0.3] }}
            transition={{ duration: 1.2, repeat: Infinity, delay: i * 0.18 }}
            className="h-1.5 w-1.5 rounded-full bg-blue-400"
          />
        ))}
      </div>
      <span className="text-xs text-t3">{stage || 'Planning retrieval strategy...'}</span>
    </div>
  );
}

function UserBubble({ content }) {
  return (
    <div className="mb-6 flex items-end justify-end gap-3">
      <div
        className="max-w-[80%] rounded-2xl rounded-br-md px-4 py-3 text-sm leading-relaxed text-white shadow-lg"
        style={{
          background: 'linear-gradient(135deg, rgba(59,130,246,0.18), rgba(59,130,246,0.12))',
          border: '1px solid rgba(59,130,246,0.22)',
          boxShadow: '0 4px 16px rgba(59,130,246,0.08)',
        }}
      >
        {content}
      </div>
      <div
        className="grid h-8 w-8 flex-shrink-0 place-items-center rounded-xl text-xs font-bold"
        style={{
          background: 'linear-gradient(135deg, rgba(99,102,241,0.2), rgba(59,130,246,0.15))',
          border: '1px solid rgba(99,102,241,0.25)',
          color: '#a5b4fc',
        }}
      >
        U
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
    <div className="mb-6 flex items-start gap-3">
      {/* Assistant avatar */}
      <div
        className="grid h-8 w-8 flex-shrink-0 place-items-center rounded-xl shadow-lg"
        style={{
          background: 'linear-gradient(135deg, #3b82f6, #06b6d4)',
          boxShadow: '0 4px 12px rgba(59,130,246,0.25)',
        }}
      >
        <span className="text-xs font-black text-white">S</span>
      </div>

      <div className="min-w-0 flex-1">
        {/* Main card */}
        <div
          className="rounded-2xl rounded-tl-md p-4 shadow-lg"
          style={{
            background: 'var(--surface-1)',
            border: '1px solid var(--border-1)',
            boxShadow: '0 4px 24px rgba(0,0,0,0.12)',
          }}
        >
          {message.error ? (
            /* Error state with inline retry */
            <div className="rounded-xl p-4"
              style={{
                background: 'rgba(239,68,68,0.06)',
                border: '1px solid rgba(239,68,68,0.15)',
                borderLeft: '3px solid rgba(239,68,68,0.5)',
              }}
            >
              <div className="flex items-start gap-3">
                <AlertTriangle size={16} className="text-red-400 flex-shrink-0 mt-0.5" />
                <div className="flex-1 min-w-0">
                  <p className="text-sm text-red-100/80 mb-2">{message.error}</p>
                  <button
                    onClick={onRegenerate}
                    className="text-xs font-medium px-3 py-1.5 rounded-lg transition-all"
                    style={{
                      background: 'rgba(239,68,68,0.1)',
                      border: '1px solid rgba(239,68,68,0.2)',
                      color: '#fca5a5',
                    }}
                  >
                    <RefreshCw size={11} className="inline mr-1.5" />
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
                <div className={`mb-4 text-sm leading-7 text-t2 ${message.streaming && message._pipelineStep >= 4 ? 'typing-cursor block' : ''}`}>
                  <MarkdownText text={message.streamText || data.message} />
                </div>
              )}

              {rows.length > 0 && <ResultTable rows={rows} />}
              {rows.length >= 2 && <ChartView rows={rows} />}

              <InsightBlock insights={data.insights} explanation={data.explanation || data.sql_explanation} />

              {/* Follow-up questions */}
              {Array.isArray(data.follow_ups) && data.follow_ups.length > 0 && !message.streaming && (
                <div className="mt-3">
                  <button
                    onClick={() => setFollowUpOpen(v => !v)}
                    className="text-xs font-medium transition-colors"
                    style={{ color: 'rgba(103,232,249,0.6)' }}
                    onMouseEnter={e => { e.currentTarget.style.color = '#67e8f9'; }}
                    onMouseLeave={e => { e.currentTarget.style.color = 'rgba(103,232,249,0.6)'; }}
                  >
                    {followUpOpen ? '▾ Hide' : '▸ Show'} follow-up questions
                  </button>
                  <AnimatePresence>
                    {followUpOpen && (
                      <motion.div
                        initial={{ height: 0, opacity: 0 }}
                        animate={{ height: 'auto', opacity: 1 }}
                        exit={{ height: 0, opacity: 0 }}
                        className="mt-2 flex flex-wrap gap-2 overflow-hidden"
                      >
                        {data.follow_ups.slice(0, 4).map((q, i) => (
                          <button
                            key={i}
                            onClick={() => window.dispatchEvent(new CustomEvent('plainsql:submit', { detail: { query: q } }))}
                            className="rounded-lg px-3 py-1.5 text-xs text-t2 transition-all"
                            style={{
                              background: 'var(--surface-1)',
                              border: '1px solid var(--border-1)',
                            }}
                            onMouseEnter={e => {
                              e.currentTarget.style.borderColor = 'rgba(6,182,212,0.3)';
                              e.currentTarget.style.color = '#fff';
                            }}
                            onMouseLeave={e => {
                              e.currentTarget.style.borderColor = 'var(--border-1)';
                              e.currentTarget.style.color = '';
                            }}
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

        {/* Action bar — clean grouping */}
        {!message.pending && !message.streaming && !message.error && (
          <div className="mt-2 flex items-center gap-0.5 pl-1">
            {/* Left group: Copy + Retry */}
            <button
              onClick={handleCopyResponse}
              className="flex items-center gap-1 rounded-lg px-2 py-1.5 text-xs text-t4 transition-all hover:bg-white/[0.05] hover:text-t2"
              aria-label="Copy response"
            >
              {copied ? <Check size={11} className="text-emerald-400" /> : <Copy size={11} />}
              {copied ? 'Copied' : 'Copy'}
            </button>
            <button
              onClick={onRegenerate}
              className="flex items-center gap-1 rounded-lg px-2 py-1.5 text-xs text-t4 transition-all hover:bg-white/[0.05] hover:text-t2"
              aria-label="Regenerate response"
            >
              <RefreshCw size={11} />
              Retry
            </button>

            <div className="flex-1" />

            {/* Right group: Feedback */}
            <div className="flex items-center gap-0.5 rounded-lg p-0.5"
              style={{ background: 'var(--surface-1)' }}>
              <button
                onClick={() => handleFeedback('up')}
                className={`rounded-md p-1.5 transition-all ${
                  message._feedback === 'up'
                    ? 'bg-emerald-400/15 text-emerald-300'
                    : 'text-t4 hover:bg-white/[0.06] hover:text-t2'
                }`}
                aria-label="Thumbs up"
              >
                <ThumbsUp size={12} />
              </button>
              <button
                onClick={() => handleFeedback('down')}
                className={`rounded-md p-1.5 transition-all ${
                  message._feedback === 'down'
                    ? 'bg-red-400/15 text-red-300'
                    : 'text-t4 hover:bg-white/[0.06] hover:text-t2'
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
