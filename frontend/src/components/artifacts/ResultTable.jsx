import { useState } from 'react';
import { motion } from 'framer-motion';
import { Table2, Download, Copy, ArrowUpDown } from 'lucide-react';
import useChatStore from '../../store/useChatStore';

function formatCell(v) {
  if (v == null) return '-';
  if (typeof v === 'number') return Number.isInteger(v) ? v.toLocaleString() : v.toFixed(2);
  return String(v);
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
    addToast('Result JSON copied', 'success');
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      className="rounded-xl overflow-hidden mb-3"
      style={{ border: '1px solid rgba(255,255,255,0.08)' }}
    >
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2.5"
        style={{ background: 'rgba(255,255,255,0.03)', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
        <div className="flex items-center gap-2">
          <Table2 size={13} className="text-white/50" />
          <span className="text-xs font-semibold text-white/60">Results</span>
          <span className="text-xs font-mono text-white/30">{rows.length} rows</span>
        </div>
        <div className="flex items-center gap-1">
          <button onClick={handleCopyJSON}
            className="flex items-center gap-1 px-2 py-1 rounded-lg text-xs text-white/40 hover:text-white hover:bg-white/10 transition-all">
            <Copy size={11} /> <span className="hidden sm:inline">JSON</span>
          </button>
          <button onClick={() => downloadCSV(rows)}
            className="flex items-center gap-1 px-2 py-1 rounded-lg text-xs text-white/40 hover:text-white hover:bg-white/10 transition-all">
            <Download size={11} /> <span className="hidden sm:inline">CSV</span>
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr style={{ background: 'rgba(255,255,255,0.025)', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
              {cols.map(col => (
                <th
                  key={col}
                  onClick={() => handleSort(col)}
                  className="text-left px-4 py-2.5 font-semibold text-white/40 cursor-pointer hover:text-white/70 transition-colors whitespace-nowrap select-none"
                >
                  <div className="flex items-center gap-1">
                    {col}
                    <ArrowUpDown size={9} className={sortCol === col ? 'text-primary' : 'text-white/20'} />
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {paged.map((row, ri) => (
              <tr
                key={ri}
                className="border-b border-white/[0.04] hover:bg-white/[0.03] transition-colors"
              >
                {cols.map(col => (
                  <td key={col} className="px-4 py-2.5 text-white/70 font-mono whitespace-nowrap max-w-xs truncate">
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
        <div className="flex items-center justify-between px-4 py-2 border-t border-white/[0.06]"
          style={{ background: 'rgba(255,255,255,0.02)' }}>
          <span className="text-xs text-white/30 font-mono">
            {page * PAGE_SIZE + 1}-{Math.min((page + 1) * PAGE_SIZE, rows.length)} of {rows.length}
          </span>
          <div className="flex gap-1">
            <button disabled={page === 0} onClick={() => setPage(p => p - 1)}
              className="px-2 py-1 rounded text-xs text-white/40 hover:text-white hover:bg-white/10 disabled:opacity-30 transition-all">
              Prev
            </button>
            <button disabled={page >= totalPages - 1} onClick={() => setPage(p => p + 1)}
              className="px-2 py-1 rounded text-xs text-white/40 hover:text-white hover:bg-white/10 disabled:opacity-30 transition-all">
              Next
            </button>
          </div>
        </div>
      )}
    </motion.div>
  );
}
