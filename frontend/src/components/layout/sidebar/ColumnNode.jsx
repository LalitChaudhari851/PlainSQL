import { memo } from 'react';
import { KeyRound, Link, Circle } from 'lucide-react';

/**
 * ColumnNode — displays a single column with type, PK/FK badges, and nullability.
 *
 * Rendering examples:
 *   🔑 id            int           PK
 *   🔗 department_id int           FK → departments
 *      salary        decimal(10,2)
 *      email         varchar(160)   ⊘ nullable
 */
const ColumnNode = memo(function ColumnNode({ column, relationship }) {
  const { name, type, isPK, isFK, nullable } = column;

  // Simplify type for display (e.g., "enum('a','b','c')" → "enum")
  const displayType = type?.includes('(')
    ? type.slice(0, type.indexOf('(')).toLowerCase()
    : type?.toLowerCase() ?? '';

  // Full type for tooltip
  const fullType = type ?? '';

  return (
    <div
      className="flex items-center gap-1.5 px-3 py-[3px] text-[11px] group/col hover:bg-white/[0.02] transition-colors rounded-sm"
      title={`${name} — ${fullType}${isPK ? ' (Primary Key)' : ''}${isFK ? ' (Foreign Key)' : ''}${nullable ? ' (Nullable)' : ''}`}
    >
      {/* Icon */}
      <span className="w-3.5 flex items-center justify-center flex-shrink-0">
        {isPK ? (
          <KeyRound size={10} className="text-amber-400/80" />
        ) : isFK ? (
          <Link size={10} className="text-cyan-400/70" />
        ) : (
          <Circle size={3} className="text-white/10" />
        )}
      </span>

      {/* Column name */}
      <span className={`font-mono truncate flex-1 min-w-0 ${
        isPK ? 'text-amber-200/80 font-medium' : isFK ? 'text-cyan-200/70' : 'text-t3'
      }`}>
        {name}
      </span>

      {/* Type badge */}
      <span className="text-[9px] font-mono text-t4 truncate max-w-[72px] flex-shrink-0" title={fullType}>
        {displayType}
      </span>

      {/* PK/FK/Nullable indicators */}
      <span className="w-10 text-right flex-shrink-0">
        {isPK && (
          <span className="text-[8px] font-bold text-amber-400/60 uppercase tracking-wider">PK</span>
        )}
        {isFK && relationship && (
          <span className="text-[8px] font-mono text-cyan-400/50 truncate" title={`→ ${relationship.referencedTable}`}>
            → {relationship.referencedTable?.slice(0, 6)}
          </span>
        )}
        {!isPK && !isFK && nullable && (
          <span className="text-[8px] text-t4 opacity-0 group-hover/col:opacity-60 transition-opacity">null</span>
        )}
      </span>
    </div>
  );
});

export default ColumnNode;
