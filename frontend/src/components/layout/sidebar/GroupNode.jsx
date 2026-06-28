import { memo, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { ChevronRight } from 'lucide-react';
import TableNode from './TableNode';

const listVariants = {
  collapsed: { height: 0, opacity: 0 },
  expanded: {
    height: 'auto',
    opacity: 1,
    transition: {
      height: { duration: 0.18, ease: [0.16, 1, 0.3, 1] },
      opacity: { duration: 0.12, delay: 0.03 },
      staggerChildren: 0.02,
      delayChildren: 0.04,
    },
  },
  exit: {
    height: 0,
    opacity: 0,
    transition: { height: { duration: 0.15 }, opacity: { duration: 0.08 } },
  },
};

const itemVariants = {
  collapsed: { opacity: 0, y: -6 },
  expanded: {
    opacity: 1,
    y: 0,
    transition: { duration: 0.16, ease: [0.16, 1, 0.3, 1] },
  },
};

/**
 * GroupNode — independently collapsible table group (e.g., "HR & Staff", "Billing").
 *
 * Each group remembers its expanded/collapsed state via the Zustand store.
 * Auto-expands when search filter is active.
 */
const GroupNode = memo(function GroupNode({
  groupName,
  tables,
  expanded,
  onToggle,
  selectedSchema,
  onSelectSchema,
  expandedTables,
  onToggleTable,
  parsedSchema,
  onTableSelected,
  searchQuery,
}) {
  const handleToggle = useCallback(() => {
    onToggle(groupName, !expanded);
  }, [groupName, expanded, onToggle]);

  // When searching, auto-expand groups that have matching tables
  const isSearchActive = searchQuery && searchQuery.length > 0;
  const effectiveExpanded = isSearchActive || expanded;

  return (
    <div className="mb-0.5" role="group" aria-label={groupName}>
      {/* Group header */}
      <button
        onClick={handleToggle}
        className="flex items-center gap-1.5 w-full px-2.5 py-[5px] text-left group/grp rounded-md hover:bg-white/[0.02] transition-colors"
        aria-expanded={effectiveExpanded}
      >
        <ChevronRight
          size={10}
          className={`text-t4 transition-transform duration-150 flex-shrink-0 ${
            effectiveExpanded ? 'rotate-90' : ''
          }`}
        />
        <span className="text-[9px] font-bold text-t4 uppercase tracking-wider group-hover/grp:text-t3 transition-colors">
          {groupName}
        </span>
        <span className="text-[9px] text-t4/50 font-mono ml-auto tabular-nums">
          {tables.length}
        </span>
      </button>

      {/* Collapsible table list */}
      <AnimatePresence initial={false}>
        {effectiveExpanded && (
          <motion.div
            variants={listVariants}
            initial="collapsed"
            animate="expanded"
            exit="exit"
            className="overflow-hidden"
          >
            <div className="space-y-px pl-1" role="group">
              {tables.map(tableName => {
                const meta = parsedSchema?.[tableName];
                return (
                  <motion.div key={tableName} variants={itemVariants}>
                    <TableNode
                      name={tableName}
                      isSelected={selectedSchema === tableName}
                      isExpanded={!!expandedTables[tableName]}
                      columns={meta?.columns ?? []}
                      relationships={meta?.relationships ?? []}
                      columnCount={meta?.columns?.length ?? 0}
                      onSelect={onSelectSchema}
                      onToggleExpand={onToggleTable}
                      onTableSelected={onTableSelected}
                      searchQuery={searchQuery}
                    />
                  </motion.div>
                );
              })}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
});

export default GroupNode;
