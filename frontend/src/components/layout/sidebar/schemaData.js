/**
 * schemaData.js — Schema parsing and metadata utilities.
 *
 * Parses the schema_text returned by the backend's /api/v1/schema endpoint
 * into structured column metadata with types, PK/FK flags, and relationships.
 *
 * When schema_text is unavailable, provides graceful fallback with table names only.
 */

// ── Table Group Assignments ────────────────────────────────
export const TABLE_GROUPS = {
  accounts: 'Core SaaS',
  contacts: 'Core SaaS',
  workspaces: 'Core SaaS',
  workspace_users: 'Core SaaS',

  departments: 'HR & Staff',
  employees: 'HR & Staff',

  plans: 'Billing',
  products: 'Billing',
  feature_catalog: 'Billing',
  subscriptions: 'Billing',
  invoices: 'Billing',
  payments: 'Billing',

  opportunities: 'Sales',

  support_tickets: 'Support & Ops',
  ticket_events: 'Support & Ops',
  incidents: 'Support & Ops',
  product_usage_daily: 'Support & Ops',
  query_audit_log: 'Support & Ops',
};

export const GROUP_ORDER = [
  'Core SaaS',
  'HR & Staff',
  'Billing',
  'Sales',
  'Support & Ops',
  'Other',
];

// ── Schema Text Parser ────────────────────────────────────

/**
 * Parses the backend's schema_text into structured per-table metadata.
 *
 * Input format (from backend get_full_schema()):
 *   Table: employees
 *   Columns:
 *     - id (int) [PRIMARY KEY]
 *     - department_id (int) [FOREIGN KEY]
 *     - salary (decimal(10,2))
 *   Relationships:
 *     - department_id → departments.department_id
 *
 * @param {string} schemaText - Raw schema text from backend
 * @returns {Object} Map of tableName → { columns: [...], relationships: [...] }
 */
export function parseSchemaText(schemaText) {
  if (!schemaText) return {};

  const result = {};
  const tableBlocks = schemaText.split(/\nTable: /);

  for (const block of tableBlocks) {
    const trimmed = block.replace(/^Table: /, '').trim();
    if (!trimmed) continue;

    const lines = trimmed.split('\n');
    const tableName = lines[0].trim();
    if (!tableName) continue;

    const columns = [];
    const relationships = [];
    let section = null;

    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (line === 'Columns:') { section = 'columns'; continue; }
      if (line === 'Relationships:') { section = 'relationships'; continue; }
      if (!line || !line.startsWith('- ')) continue;

      const content = line.slice(2).trim();

      if (section === 'columns') {
        const col = parseColumnLine(content);
        if (col) columns.push(col);
      } else if (section === 'relationships') {
        const rel = parseRelationshipLine(content);
        if (rel) relationships.push(rel);
      }
    }

    result[tableName] = { columns, relationships };
  }

  return result;
}

/**
 * Parses a single column line like:
 *   "id (int) [PRIMARY KEY]"
 *   "account_name (varchar(160))"
 *   "segment (enum('startup','mid_market','enterprise','strategic'))"
 */
function parseColumnLine(line) {
  // Match: name (type) [optional flags]
  // Type can contain parentheses (e.g., varchar(160), enum('a','b'))
  const nameEnd = line.indexOf(' (');
  if (nameEnd === -1) return null;

  const name = line.slice(0, nameEnd).trim();

  // Find the matching closing paren for the type
  let depth = 0;
  let typeEnd = -1;
  for (let i = nameEnd + 1; i < line.length; i++) {
    if (line[i] === '(') depth++;
    if (line[i] === ')') {
      depth--;
      if (depth === 0) { typeEnd = i; break; }
    }
  }
  if (typeEnd === -1) return null;

  const type = line.slice(nameEnd + 2, typeEnd).trim();
  const flags = line.slice(typeEnd + 1).trim();

  return {
    name,
    type,
    isPK: flags.includes('PRIMARY KEY'),
    isFK: flags.includes('FOREIGN KEY'),
    nullable: !flags.includes('NOT NULL'),
  };
}

/**
 * Parses a relationship line like:
 *   "department_id → departments.department_id"
 */
function parseRelationshipLine(line) {
  const parts = line.split('→').map(s => s.trim());
  if (parts.length !== 2) return null;

  const [refTable, refCol] = parts[1].includes('.')
    ? parts[1].split('.')
    : [parts[1], null];

  return {
    column: parts[0],
    referencedTable: refTable,
    referencedColumn: refCol,
  };
}

// ── Grouping Utilities ────────────────────────────────────

/**
 * Groups tables by category in a consistent order.
 * @param {string[]} tables - Array of table names
 * @param {string} [filter] - Optional search filter
 * @returns {Array<[string, string[]]>} Ordered array of [groupName, tableNames[]]
 */
export function groupTables(tables, filter) {
  const filtered = filter
    ? tables.filter(t => t.toLowerCase().includes(filter.toLowerCase()))
    : tables;

  const groups = {};
  filtered.forEach(t => {
    const group = TABLE_GROUPS[t] || 'Other';
    if (!groups[group]) groups[group] = [];
    groups[group].push(t);
  });

  // Sort by defined order
  const sorted = [];
  GROUP_ORDER.forEach(name => {
    if (groups[name]) sorted.push([name, groups[name]]);
  });
  // Append any unlisted groups
  Object.entries(groups).forEach(([name, items]) => {
    if (!GROUP_ORDER.includes(name)) sorted.push([name, items]);
  });

  return sorted;
}

/**
 * Gets column count for a table from parsed metadata.
 * Falls back to 0 if metadata unavailable.
 */
export function getColumnCount(parsedSchema, tableName) {
  return parsedSchema?.[tableName]?.columns?.length ?? 0;
}

/**
 * Gets relationship count for a table from parsed metadata.
 */
export function getRelationshipCount(parsedSchema, tableName) {
  return parsedSchema?.[tableName]?.relationships?.length ?? 0;
}
