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
  'rgba(59,130,246,0.85)',
  'rgba(6,182,212,0.85)',
  'rgba(167,139,250,0.85)',
  'rgba(34,197,94,0.85)',
  'rgba(245,158,11,0.85)',
  'rgba(239,68,68,0.85)',
];

const GRADIENTS = [
  ['rgba(59,130,246,0.3)', 'rgba(59,130,246,0.02)'],
  ['rgba(6,182,212,0.3)', 'rgba(6,182,212,0.02)'],
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
        ? 'rgba(59,130,246,0.08)'
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
        font: { family: 'Inter', size: 11 },
        boxWidth: 12,
        boxHeight: 12,
        borderRadius: 3,
        useBorderRadius: true,
        padding: 12,
      },
    },
    tooltip: {
      backgroundColor: 'rgba(6,9,18,0.95)',
      borderColor: 'rgba(255,255,255,0.1)',
      borderWidth: 1,
      titleColor: 'rgba(255,255,255,0.9)',
      bodyColor: 'rgba(255,255,255,0.65)',
      padding: 12,
      cornerRadius: 8,
      titleFont: { weight: '600' },
      displayColors: true,
      boxPadding: 4,
    },
  },
  scales: {
    x: {
      ticks: { color: 'rgba(255,255,255,0.3)', font: { size: 10 }, maxRotation: 45 },
      grid: { color: 'rgba(255,255,255,0.03)', drawBorder: false },
    },
    y: {
      ticks: { color: 'rgba(255,255,255,0.3)', font: { size: 10 } },
      grid: { color: 'rgba(255,255,255,0.04)', drawBorder: false },
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
      className="rounded-xl overflow-hidden mb-4"
      style={{ border: '1px solid var(--border-1)' }}
    >
      <div
        className="flex items-center justify-between px-4 py-2.5"
        style={{
          background: 'var(--surface-1)',
          borderBottom: '1px solid var(--border-1)',
        }}
      >
        <div className="flex items-center gap-2">
          <BarChart3 size={13} className="text-t3" />
          <span className="text-xs font-semibold text-t2">Visualization</span>
        </div>
        <div className="flex gap-1 p-0.5 rounded-lg" style={{ background: 'var(--surface-1)' }}>
          {[
            { key: 'bar', icon: BarChart3, label: 'Bar' },
            { key: 'line', icon: TrendingUp, label: 'Line' },
          ].map(t => (
            <button
              key={t.key}
              onClick={() => setType(t.key)}
              className="flex items-center gap-1 px-2.5 py-1 rounded-md text-xs transition-all"
              style={type === t.key
                ? { background: 'rgba(59,130,246,0.15)', color: '#60a5fa', border: '1px solid rgba(59,130,246,0.25)' }
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
      <div className="p-4" style={{ height: 280 }}>
        {type === 'bar'
          ? <Bar data={dataset} options={CHART_OPTIONS} />
          : <Line data={dataset} options={CHART_OPTIONS} />}
      </div>
    </motion.div>
  );
}
