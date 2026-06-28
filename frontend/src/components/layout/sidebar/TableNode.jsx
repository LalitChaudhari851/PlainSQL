import { memo, useState, useCallback, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { ChevronRight, Table2, Sparkles, Columns3 } from 'lucide-react';
import ColumnNode from './ColumnNode';
import AIActionsMenu from './AIActionsMenu';
import ContextMenu from './ContextMenu';
import SearchHighlight from './SearchHighlight';

const columnPanelVariants = {
  hidden: { height: 0, opacity: 0 },
  visible: {
    height: 'auto',
    opacity: 1,
    transition: { duration: 0.18, ease: [0.16, 1, 0.3, 1] },
  },
  exit: {
    height: 0,
    opacity: 0,
    transition: { duration: 0.14, ease: [0.16, 1, 0.3, 1] },
  },
};

/**
 * TableNode — a single table row in the schema tree.
 *
 * Features:
 * - Click body → select table
 * - Click chevron → expand/collapse columns (persisted, doesn't change selection)
 * - Hover → reveals AI sparkles button
 * - Right-click → context menu
 * - Real column metadata with types, PK/FK badges
 * - Search highlighting
 * - Full keyboard accessibility
 */
const TableNode = memo(function TableNode({
  name,
  isSelected,
  isExpanded,
  columns,
  relationships,
  columnCount,
  onSelect,
  onToggleExpand,
  onTableSelected,
  searchQuery,
}) {
  const [showAI, setShowAI] = useState(false);
  const [contextMenu, setContextMenu] = useState(null);
  const aiButtonRef = useRef(null);

  const handleSelect = useCallback(() => {
    onSelect(name);
    onTableSelected?.();
  }, [name, onSelect, onTableSelected]);

  const handleToggleColumns = useCallback((e) => {
    e.stopPropagation();
    onToggleExpand(name);
  }, [name, onToggleExpand]);

  const handleContextMenu = useCallback((e) => {
    e.preventDefault();
    setContextMenu({ x: e.clientX, y: e.clientY });
  }, []);

  const handleKeyDown = useCallback((e) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      handleSelect();
    }
    if (e.key === 'ArrowRight' && !isExpanded) {
      e.preventDefault();
      onToggleExpand(name);
    }
    if (e.key === 'ArrowLeft' && isExpanded) {
      e.preventDefault();
      onToggleExpand(name);
    }
  }, [name, isExpanded, handleSelect, onToggleExpand]);

  // Build relationship map for columns
  const relMap = {};
  if (relationships) {
    relationships.forEach(r => { relMap[r.column] = r; });
  }

  return (
    <div
      role="treeitem"
      aria-selected={isSelected}
      aria-expanded={isExpanded}
      aria-label={`${name}, ${columnCount} columns`}
    >
      {/* Main table row */}
      <div
        onClick={handleSelect}
        onContextMenu={handleContextMenu}
        onKeyDown={handleKeyDown}
        onMouseEnter={() => setShowAI(true)}
        onMouseLeave={() => { setShowAI(false); }}
        tabIndex={0}
        role="button"
        className={`
          table-item relative w-full flex items-center justify-between
          px-2.5 py-[7px] text-xs text-left rounded-md cursor-pointer
          transition-all duration-150 ease-out group/row
          ${isSelected
            ? 'table-item-active text-white font-medium'
            : 'text-t3 hover:text-t2 hover:bg-white/[0.04]'
          }
        `}
      >
        <div className="flex items-center gap-2 min-w-0 flex-1">
          {/* Expand chevron */}
          <button
            onClick={handleToggleColumns}
            className="w-4 h-4 flex items-center justify-center flex-shrink-0 rounded hover:bg-white/[0.08] transition-colors"
            tabIndex={-1}
            aria-label={`${isExpanded ? 'Collapse' : 'Expand'} columns for ${name}`}
          >
            <ChevronRight
              size={10}
              className={`text-t4 transition-transform duration-150 ${isExpanded ? 'rotate-90' : ''}`}
            />
          </button>

          <Table2
            size={13}
            className={`flex-shrink-0 transition-colors duration-150 ${
              isSelected ? 'text-brand-light' : 'text-t4 group-hover/row:text-t3'
            }`}
          />
          <span className="truncate">
            <SearchHighlight text={name} query={searchQuery} />
          </span>
        </div>

        <div className="flex items-center gap-1 flex-shrink-0">
          {/* Column count metadata */}
          <span className={`text-[9px] font-mono tabular-nums transition-colors duration-150 ${
            isSelected ? 'text-brand-light/60' : 'text-t4'
          }`}>
            {columnCount > 0 ? `${columnCount} cols` : ''}
          </span>

          {/* AI sparkles button — visible on hover */}
          <div className="relative" ref={aiButtonRef}>
            <button
              onClick={(e) => { e.stopPropagation(); setShowAI(v => !v); }}
              className={`
                w-5 h-5 rounded flex items-center justify-center
                transition-all duration-150
                ${showAI
                  ? 'text-brand-light bg-brand-dim/30'
                  : 'text-t4 opacity-0 group-hover/row:opacity-100 hover:text-brand-light hover:bg-brand-dim/20'
                }
              `}
              tabIndex={-1}
              aria-label={`AI actions for ${name}`}
              aria-haspopup="menu"
            >
              <Sparkles size={11} />
            </button>
          </div>
        </div>
      </div>

      {/* AI Actions Menu */}
      <AnimatePresence>
        {showAI && (
          <div className="relative">
            <AIActionsMenu
              tableName={name}
              onClose={() => setShowAI(false)}
              anchorRef={aiButtonRef}
            />
          </div>
        )}
      </AnimatePresence>

      {/* Context Menu */}
      {contextMenu && (
        <ContextMenu
          tableName={name}
          position={contextMenu}
          onClose={() => setContextMenu(null)}
          onExpandColumns={() => { if (!isExpanded) onToggleExpand(name); }}
          onAskAI={() => setShowAI(true)}
        />
      )}

      {/* Expandable columns panel */}
      <AnimatePresence initial={false}>
        {isExpanded && columns && columns.length > 0 && (
          <motion.div
            key="columns"
            variants={columnPanelVariants}
            initial="hidden"
            animate="visible"
            exit="exit"
            className="overflow-hidden"
          >
            <div className="ml-[22px] mr-1 mb-1 py-1 border-l border-white/[0.06]">
              <div className="flex items-center gap-1.5 px-3 py-1 mb-0.5">
                <Columns3 size={9} className="text-t4" />
                <span className="text-[9px] font-bold text-t4 uppercase tracking-wider">
                  Columns ({columns.length})
                </span>
              </div>
              {columns.map((col) => (
                <ColumnNode
                  key={col.name}
                  column={col}
                  relationship={relMap[col.name]}
                />
              ))}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
});

export default TableNode;
