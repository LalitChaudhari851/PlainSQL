import React, { useState } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { Copy, Database, RefreshCw, Rows3, Sigma, ThumbsDown, ThumbsUp } from 'lucide-react';
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
    .replace(/`([^`]+)`/g, '<code class="font-mono text-cyan-300 bg-white/10 px-1 rounded text-xs">$1</code>')
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
    <div className="mb-3 grid gap-2 sm:grid-cols-3">
      {cards.map(({ icon: Icon, label, value }) => (
        <div key={label} className="rounded-xl border border-white/[0.07] bg-white/[0.035] p-3">
          <div className="mb-2 flex items-center gap-2 text-white/35">
            <Icon size={13} />
            <span className="text-[11px] font-semibold uppercase tracking-wide">{label}</span>
          </div>
          <p className="truncate font-mono text-lg font-semibold text-white">{value}</p>
        </div>
      ))}
    </div>
  );
}

function ThinkingStatus({ stage }) {
  return (
    <div className="mb-3 flex items-center gap-2.5 rounded-xl border border-white/[0.06] bg-white/[0.025] px-3 py-2.5">
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
      <span className="text-xs text-white/50">{stage || 'Planning retrieval strategy...'}</span>
    </div>
  );
}

function UserBubble({ content }) {
  return (
    <div className="mb-5 flex items-end justify-end gap-3">
      <div className="max-w-[82%] rounded-2xl rounded-br-md border border-blue-400/20 bg-blue-500/16 px-4 py-3 text-sm leading-relaxed text-white shadow-lg shadow-blue-950/20">
        {content}
      </div>
      <div className="grid h-8 w-8 flex-shrink-0 place-items-center rounded-xl border border-white/10 bg-white/[0.06] text-xs font-bold text-white/75">
        U
      </div>
    </div>
  );
}

function AssistantBubble({ message, chatId, onRegenerate }) {
  const addToast = useChatStore(s => s.addToast);
  const setFeedback = useChatStore(s => s.setFeedback);
  const [followUpOpen, setFollowUpOpen] = useState(false);

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
    addToast('Response copied', 'success');
  };

  return (
    <div className="mb-7 flex items-start gap-3">
      <div className="grid h-8 w-8 flex-shrink-0 place-items-center rounded-xl bg-gradient-to-br from-blue-500 to-cyan-400 shadow-lg shadow-blue-500/20">
        <span className="text-xs font-black text-white">S</span>
      </div>

      <div className="min-w-0 flex-1">
        <div className="rounded-2xl rounded-tl-md border border-white/[0.08] bg-white/[0.04] p-4 shadow-xl shadow-black/20 backdrop-blur-xl">
          {message.error ? (
            <div className="rounded-xl border border-red-400/20 bg-red-400/[0.07] p-3 text-sm text-red-100/80">
              {message.error}
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
                <div className={`mb-3 text-sm leading-7 text-white/86 ${message.streaming && message._pipelineStep >= 4 ? 'typing-cursor block' : ''}`}>
                  <MarkdownText text={message.streamText || data.message} />
                </div>
              )}

              {rows.length > 0 && <ResultTable rows={rows} />}
              {rows.length >= 2 && <ChartView rows={rows} />}

              <InsightBlock insights={data.insights} explanation={data.explanation || data.sql_explanation} />

              {Array.isArray(data.follow_ups) && data.follow_ups.length > 0 && !message.streaming && (
                <div className="mt-3">
                  <button
                    onClick={() => setFollowUpOpen(v => !v)}
                    className="text-xs font-medium text-cyan-200/60 transition-colors hover:text-cyan-100"
                  >
                    {followUpOpen ? 'Hide' : 'Show'} follow-up questions
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
                            className="rounded-lg border border-white/[0.08] bg-white/[0.04] px-3 py-1.5 text-xs text-white/62 transition-all hover:border-cyan-400/30 hover:text-white"
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

        {!message.pending && !message.streaming && !message.error && (
          <div className="mt-2 flex items-center gap-1 pl-1">
            <button onClick={handleCopyResponse} className="flex items-center gap-1 rounded-lg px-2 py-1 text-xs text-white/34 transition-all hover:bg-white/[0.05] hover:text-white/70">
              <Copy size={11} /> Copy
            </button>
            <button onClick={onRegenerate} className="flex items-center gap-1 rounded-lg px-2 py-1 text-xs text-white/34 transition-all hover:bg-white/[0.05] hover:text-white/70">
              <RefreshCw size={11} /> Retry
            </button>
            <div className="flex-1" />
            <button onClick={() => handleFeedback('up')} className={`rounded-lg p-1.5 transition-all ${message._feedback === 'up' ? 'bg-emerald-400/10 text-emerald-300' : 'text-white/28 hover:bg-white/[0.05] hover:text-white/70'}`}>
              <ThumbsUp size={12} />
            </button>
            <button onClick={() => handleFeedback('down')} className={`rounded-lg p-1.5 transition-all ${message._feedback === 'down' ? 'bg-red-400/10 text-red-300' : 'text-white/28 hover:bg-white/[0.05] hover:text-white/70'}`}>
              <ThumbsDown size={12} />
            </button>
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
