import { useEffect, useRef, useState } from 'react';
import { motion } from 'framer-motion';
import { BarChart3 } from 'lucide-react';
import {
  Chart as ChartJS,
  CategoryScale, LinearScale, BarElement, LineElement,
  PointElement, ArcElement, Tooltip, Legend, Filler
} from 'chart.js';
import { Bar, Line } from 'react-chartjs-2';

ChartJS.register(CategoryScale, LinearScale, BarElement, LineElement, PointElement, ArcElement, Tooltip, Legend, Filler);

const COLORS = ['#3b82f6', '#06b6d4', '#a78bfa', '#22c55e', '#f59e0b', '#ef4444'];

function inferType(rows, cols) {
  return cols.some(c => /date|month|year|day|time/i.test(c)) ? 'line' : 'bar';
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
      label: valueCol,
      data,
      borderColor: COLORS[0],
      backgroundColor: type === 'line' ? 'rgba(59,130,246,0.1)' : COLORS.map((c, i) => c + 'cc'),
      pointBackgroundColor: COLORS[1],
      borderWidth: 2,
      borderRadius: type === 'bar' ? 6 : 0,
      tension: 0.38,
      fill: type === 'line',
      pointRadius: type === 'line' ? 3 : 0,
    }]
  };
}

const CHART_OPTIONS = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: { labels: { color: 'rgba(255,255,255,0.4)', font: { family: 'Inter', size: 11 } } },
    tooltip: {
      backgroundColor: '#0d1526',
      borderColor: 'rgba(255,255,255,0.1)',
      borderWidth: 1,
      titleColor: 'rgba(255,255,255,0.8)',
      bodyColor: 'rgba(255,255,255,0.6)',
      padding: 10,
    }
  },
  scales: {
    x: { ticks: { color: 'rgba(255,255,255,0.35)', font: { size: 10 } }, grid: { color: 'rgba(255,255,255,0.04)' } },
    y: { ticks: { color: 'rgba(255,255,255,0.35)', font: { size: 10 } }, grid: { color: 'rgba(255,255,255,0.05)' } },
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
      className="rounded-xl overflow-hidden mb-3"
      style={{ border: '1px solid rgba(255,255,255,0.08)' }}
    >
      <div className="flex items-center justify-between px-4 py-2.5"
        style={{ background: 'rgba(255,255,255,0.03)', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
        <div className="flex items-center gap-2">
          <BarChart3 size={13} className="text-white/50" />
          <span className="text-xs font-semibold text-white/60">Chart</span>
        </div>
        <div className="flex gap-1">
          {['bar', 'line'].map(t => (
            <button key={t} onClick={() => setType(t)}
              className="px-2 py-1 rounded-lg text-xs transition-all"
              style={type === t
                ? { background: 'rgba(59,130,246,0.2)', color: '#60a5fa', border: '1px solid rgba(59,130,246,0.3)' }
                : { color: 'rgba(255,255,255,0.35)', border: '1px solid rgba(255,255,255,0.06)' }
              }>
              {t.charAt(0).toUpperCase() + t.slice(1)}
            </button>
          ))}
        </div>
      </div>
      <div className="p-4" style={{ height: 220 }}>
        {type === 'bar'
          ? <Bar data={dataset} options={CHART_OPTIONS} />
          : <Line data={dataset} options={CHART_OPTIONS} />}
      </div>
    </motion.div>
  );
}
