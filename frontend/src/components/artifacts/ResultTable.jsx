import { useState } from 'react';
import { motion } from 'framer-motion';
import { Table2, Download, Copy, ArrowUpDown, ChevronLeft, ChevronRight } from 'lucide-react';
import useChatStore from '../../store/useChatStore';

function formatCell(v) {
  if (v == null) return <span className="text-t4 italic text-xs">NULL</span>;
  if (typeof v === 'number') return Number.isInteger(v) ? v.toLocaleString() : v.toFixed(2);
  return String(v);
}

function isNumericCol(rows, col) {
  return rows.some(r => r[col] != null && Number.isFinite(Number(r[col])));
}

function downloadCSV(rows) {
  if (!rows.length) return;
  const cols = Object.keys(rows[0]);
  const csv = [cols.join(','), ...rows.map(r => cols.map(c => JSON.stringify(r[c] ?? '')).join(','))].join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'plainsql_result.csv';
  a.click();
}

export default function ResultTable({ rows }) {
  const addToast = useChatStore(s => s.addToast);
  const [sortCol, setSortCol] = useState(null);
  const [sortDir, setSortDir] = useState('asc');
  const [page, setPage] = useState(0);
  const PAGE_SIZE = 15;

  if (!rows?.length) return null;
  const cols = Object.keys(rows[0]);
  const numericCols = new Set(cols.filter(c => isNumericCol(rows, c)));

  const sorted = sortCol
    ? [...rows].sort((a, b) => {
        const av = a[sortCol], bv = b[sortCol];
        const cmp = typeof av === 'number' ? av - bv : String(av ?? '').localeCompare(String(bv ?? ''));
        return sortDir === 'asc' ? cmp : -cmp;
      })
    : rows;

  const paged = sorted.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  const totalPages = Math.ceil(rows.length / PAGE_SIZE);

  const handleSort = (col) => {
    if (sortCol === col) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    else { setSortCol(col); setSortDir('asc'); }
  };

  const handleCopyJSON = async () => {
    await navigator.clipboard.writeText(JSON.stringify(rows, null, 2)).catch(() => {});
    addToast('Result JSON copied to clipboard', 'success');
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      className="rounded-xl overflow-hidden mb-4 border border-border-1"
    >
      {/* Header */}
      <div
        className="flex items-center justify-between px-4 py-2.5"
        style={{
          background: 'var(--surface-05)',
          borderBottom: '1px solid var(--border-1)',
        }}
      >
        <div className="flex items-center gap-2">
          <Table2 size={13} className="text-t3" />
          <span className="text-xs font-bold text-t2">Results</span>
          <span className="text-[10px] text-t4 font-mono font-medium bg-white/[0.03] border border-white/[0.04] px-1.5 py-0.2 rounded-sm tabular-nums">{rows.length} rows</span>
        </div>
        <div className="flex items-center gap-1">
          <button
            onClick={handleCopyJSON}
            className="flex items-center gap-1 px-2.5 py-1 rounded-lg text-xs font-semibold text-t3 hover:text-white hover:bg-white/10 transition-all"
            aria-label="Copy as JSON"
          >
            <Copy size={11} /> <span className="hidden sm:inline">JSON</span>
          </button>
          <button
            onClick={() => downloadCSV(rows)}
            className="flex items-center gap-1 px-2.5 py-1 rounded-lg text-xs font-semibold text-t3 hover:text-white hover:bg-white/10 transition-all"
            aria-label="Download CSV"
          >
            <Download size={11} /> <span className="hidden sm:inline">CSV</span>
          </button>
        </div>
      </div>

      {/* Table container */}
      <div className="overflow-x-auto max-h-[420px]">
        <table className="w-full text-xs table-zebra">
          <thead className="sticky top-0 z-10">
            <tr style={{ background: 'var(--surface-0)', borderBottom: '1px solid var(--border-1)' }}>
              {cols.map(col => (
                <th
                  key={col}
                  onClick={() => handleSort(col)}
                  className={`
                    px-4 py-2.5 font-bold text-t3 cursor-pointer
                    hover:text-t2 transition-colors whitespace-nowrap select-none
                    ${numericCols.has(col) ? 'text-right' : 'text-left'}
                  `}
                >
                  <div className={`flex items-center gap-1 ${numericCols.has(col) ? 'justify-end' : ''}`}>
                    <span>{col.replace(/_/g, ' ')}</span>
                    <ArrowUpDown size={9} className={sortCol === col ? 'text-brand-light' : 'text-t5'} />
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {paged.map((row, ri) => (
              <tr
                key={ri}
                className="transition-colors"
                style={{ borderBottom: '1px solid var(--border-0)' }}
              >
                {cols.map(col => (
                  <td
                    key={col}
                    className={`
                      px-4 py-2.5 font-mono whitespace-nowrap max-w-xs truncate
                      ${numericCols.has(col) ? 'text-right text-t1 tabular-nums' : 'text-t2'}
                    `}
                  >
                    {formatCell(row[col])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div
          className="flex items-center justify-between px-4 py-2.5"
          style={{
            background: 'var(--surface-05)',
            borderTop: '1px solid var(--border-1)',
          }}
        >
          <span className="text-xs text-t4 font-mono font-medium tabular-nums">
            {page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, rows.length)} of {rows.length}
          </span>
          <div className="flex items-center gap-2">
            <button
              disabled={page === 0}
              onClick={() => setPage(p => p - 1)}
              className="w-7 h-7 rounded-lg flex items-center justify-center text-t3 hover:text-white hover:bg-white/10 disabled:opacity-25 disabled:cursor-not-allowed transition-all focus-ring"
              aria-label="Previous page"
            >
              <ChevronLeft size={14} />
            </button>
            <span className="text-xs text-t3 font-mono font-bold tabular-nums px-1 select-none">
              {page + 1} / {totalPages}
            </span>
            <button
              disabled={page >= totalPages - 1}
              onClick={() => setPage(p => p + 1)}
              className="w-7 h-7 rounded-lg flex items-center justify-center text-t3 hover:text-white hover:bg-white/10 disabled:opacity-25 disabled:cursor-not-allowed transition-all focus-ring"
              aria-label="Next page"
            >
              <ChevronRight size={14} />
            </button>
          </div>
        </div>
      )}
    </motion.div>
  );
}
