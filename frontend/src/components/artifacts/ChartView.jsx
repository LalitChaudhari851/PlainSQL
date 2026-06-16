import { useState } from 'react';
import { motion } from 'framer-motion';
import { BarChart3, TrendingUp } from 'lucide-react';
import {
  Chart as ChartJS,
  CategoryScale, LinearScale, BarElement, LineElement,
  PointElement, ArcElement, Tooltip, Legend, Filler
} from 'chart.js';
import { Bar, Line } from 'react-chartjs-2';

ChartJS.register(CategoryScale, LinearScale, BarElement, LineElement, PointElement, ArcElement, Tooltip, Legend, Filler);

const COLORS = [
  'rgba(99,102,241,0.85)',  // brand indigo
  'rgba(6,182,212,0.85)',  // cyan
  'rgba(167,139,250,0.85)', // violet
  'rgba(52,211,153,0.85)',  // success (emerald)
  'rgba(251,191,36,0.85)',  // warning
  'rgba(248,113,113,0.85)', // danger
];

function inferType(rows, cols) {
  return cols.some(c => /date|month|year|day|time|quarter|week/i.test(c)) ? 'line' : 'bar';
}

function buildDataset(rows, type) {
  const cols = Object.keys(rows[0] ?? {});
  const numericCols = cols.filter(c => rows.every(r => r[c] == null || !isNaN(Number(r[c]))));
  const labelCol = cols.find(c => !numericCols.includes(c)) ?? cols[0];
  const valueCol = numericCols[0] ?? cols[1] ?? cols[0];
  const labels = rows.slice(0, 24).map(r => String(r[labelCol] ?? '').slice(0, 30));
  const data = rows.slice(0, 24).map(r => Number(r[valueCol] ?? 0));

  return {
    labels,
    datasets: [{
      label: valueCol.replace(/_/g, ' '),
      data,
      borderColor: COLORS[0],
      backgroundColor: type === 'line'
        ? 'rgba(99,102,241,0.08)'
        : COLORS.map(c => c),
      pointBackgroundColor: COLORS[1],
      borderWidth: 2,
      borderRadius: type === 'bar' ? 6 : 0,
      tension: 0.4,
      fill: type === 'line',
      pointRadius: type === 'line' ? 3 : 0,
      pointHoverRadius: 6,
    }]
  };
}

const CHART_OPTIONS = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: {
      display: true,
      position: 'top',
      align: 'end',
      labels: {
        color: 'rgba(255,255,255,0.4)',
        font: { family: 'Inter', size: 11, weight: '500' },
        boxWidth: 10,
        boxHeight: 10,
        borderRadius: 2,
        useBorderRadius: true,
        padding: 12,
      },
    },
    tooltip: {
      backgroundColor: 'var(--surface-05)',
      borderColor: 'var(--border-1)',
      borderWidth: 1,
      titleColor: 'rgba(255,255,255,0.95)',
      bodyColor: 'rgba(255,255,255,0.7)',
      padding: 10,
      cornerRadius: 6,
      titleFont: { weight: '600', family: 'Inter', size: 11 },
      bodyFont: { family: 'Inter', size: 11 },
      displayColors: true,
      boxPadding: 4,
    },
  },
  scales: {
    x: {
      ticks: { color: 'rgba(255,255,255,0.3)', font: { size: 10, family: 'Inter' }, maxRotation: 45 },
      grid: { color: 'rgba(255,255,255,0.02)', drawBorder: false },
    },
    y: {
      ticks: { color: 'rgba(255,255,255,0.3)', font: { size: 10, family: 'Inter' } },
      grid: { color: 'rgba(255,255,255,0.03)', drawBorder: false },
    },
  },
};

export default function ChartView({ rows }) {
  const cols = Object.keys(rows?.[0] ?? {});
  const numericCols = cols.filter(c => rows.every(r => r[c] == null || !isNaN(Number(r[c]))));
  const [type, setType] = useState(() => inferType(rows, cols));

  if (!rows?.length || !numericCols.length || cols.length < 2) return null;

  const dataset = buildDataset(rows, type);

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      className="rounded-xl overflow-hidden mb-4 border border-border-1"
    >
      <div
        className="flex items-center justify-between px-4 py-2.5"
        style={{
          background: 'var(--surface-05)',
          borderBottom: '1px solid var(--border-1)',
        }}
      >
        <div className="flex items-center gap-2">
          <BarChart3 size={13} className="text-t3" />
          <span className="text-xs font-bold text-t2 font-sans">Visualization</span>
        </div>
        <div className="flex gap-1 p-0.5 rounded-lg bg-surface-1 border border-border-1">
          {[
            { key: 'bar', icon: BarChart3, label: 'Bar' },
            { key: 'line', icon: TrendingUp, label: 'Line' },
          ].map(t => (
            <button
              key={t.key}
              onClick={() => setType(t.key)}
              className="flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-semibold transition-all focus-ring"
              style={type === t.key
                ? { background: 'var(--brand-dim)', color: 'var(--brand-light)', border: '1px solid rgba(99,102,241,0.2)' }
                : { color: 'rgba(255,255,255,0.35)', border: '1px solid transparent' }
              }
              aria-label={`${t.label} chart`}
            >
              <t.icon size={11} />
              {t.label}
            </button>
          ))}
        </div>
      </div>
      <div className="p-4 bg-white/[0.01]" style={{ height: 280 }}>
        {type === 'bar'
          ? <Bar data={dataset} options={CHART_OPTIONS} />
          : <Line data={dataset} options={CHART_OPTIONS} />}
      </div>
    </motion.div>
  );
}
