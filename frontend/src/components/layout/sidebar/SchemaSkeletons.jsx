/**
 * SchemaSkeletons — animated loading placeholders shown while schema data loads.
 * Uses the existing .skeleton CSS class defined in index.css.
 */
export default function SchemaSkeletons() {
  return (
    <div className="px-3 py-2 space-y-3" aria-label="Loading schema" role="status">
      {/* Folder skeleton */}
      <div className="flex items-center gap-2 px-1">
        <div className="skeleton w-4 h-4 rounded" />
        <div className="skeleton h-3 rounded flex-1 max-w-[100px]" />
      </div>
      {/* Table rows skeleton */}
      <div className="space-y-1.5 pl-4">
        {[88, 72, 96, 64, 80].map((w, i) => (
          <div key={i} className="flex items-center gap-2">
            <div className="skeleton w-3 h-3 rounded" />
            <div className="skeleton h-2.5 rounded" style={{ width: `${w}px` }} />
            <div className="skeleton h-2 rounded w-8 ml-auto" />
          </div>
        ))}
      </div>
      {/* Second group skeleton */}
      <div className="flex items-center gap-2 px-1 pt-1">
        <div className="skeleton w-3 h-3 rounded" />
        <div className="skeleton h-2 rounded w-12" />
      </div>
      <div className="space-y-1.5 pl-4">
        {[76, 60, 84].map((w, i) => (
          <div key={i} className="flex items-center gap-2">
            <div className="skeleton w-3 h-3 rounded" />
            <div className="skeleton h-2.5 rounded" style={{ width: `${w}px` }} />
            <div className="skeleton h-2 rounded w-8 ml-auto" />
          </div>
        ))}
      </div>
    </div>
  );
}
