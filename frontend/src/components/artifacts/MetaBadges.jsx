import { Tag, Clock, Hash, Cpu, Search } from 'lucide-react';

const intentColors = {
  analytical: { bg: 'rgba(59,130,246,0.12)', border: 'rgba(59,130,246,0.25)', text: '#60a5fa' },
  lookup:     { bg: 'rgba(6,182,212,0.12)',  border: 'rgba(6,182,212,0.25)',  text: '#22d3ee' },
  aggregate:  { bg: 'rgba(167,139,250,0.12)', border: 'rgba(167,139,250,0.25)', text: '#c4b5fd' },
  default:    { bg: 'rgba(255,255,255,0.06)', border: 'rgba(255,255,255,0.1)', text: 'rgba(255,255,255,0.6)' },
};

function Badge({ icon: Icon, label, value, colors }) {
  return (
    <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs"
      style={{ background: colors.bg, border: `1px solid ${colors.border}` }}>
      <Icon size={10} style={{ color: colors.text }} />
      <span style={{ color: 'rgba(255,255,255,0.4)' }}>{label}</span>
      <span className="font-semibold" style={{ color: colors.text }}>{value}</span>
    </div>
  );
}

export default function MetaBadges({ intent, executionTimeMs, rowCount }) {
  const colors = intentColors[intent?.toLowerCase()] ?? intentColors.default;
  const badges = [];

  if (intent) badges.push({ icon: Tag, label: 'Intent', value: intent, colors });
  if (executionTimeMs != null) badges.push({
    icon: Clock, label: 'Latency', value: `${Math.round(executionTimeMs)}ms`,
    colors: { bg: 'rgba(34,197,94,0.1)', border: 'rgba(34,197,94,0.2)', text: '#4ade80' }
  });
  if (rowCount != null) badges.push({
    icon: Hash, label: 'Rows', value: rowCount,
    colors: { bg: 'rgba(255,255,255,0.05)', border: 'rgba(255,255,255,0.08)', text: 'rgba(255,255,255,0.5)' }
  });

  if (!badges.length) return null;

  return (
    <div className="flex flex-wrap gap-1.5 mb-3">
      <Badge icon={Search} label="RAG" value="Active" colors={{ bg: 'rgba(6,182,212,0.1)', border: 'rgba(6,182,212,0.2)', text: '#22d3ee' }} />
      {badges.map((b, i) => <Badge key={i} {...b} />)}
    </div>
  );
}
