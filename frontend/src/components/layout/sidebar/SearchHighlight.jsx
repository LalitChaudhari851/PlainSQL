import { memo } from 'react';

/**
 * SearchHighlight — wraps matching text segments in <mark> elements.
 * Used throughout the schema tree to highlight search matches.
 */
const SearchHighlight = memo(function SearchHighlight({ text, query }) {
  if (!query || !text) return <>{text}</>;

  const lowerText = text.toLowerCase();
  const lowerQuery = query.toLowerCase();
  const idx = lowerText.indexOf(lowerQuery);

  if (idx === -1) return <>{text}</>;

  const before = text.slice(0, idx);
  const match = text.slice(idx, idx + query.length);
  const after = text.slice(idx + query.length);

  return (
    <>
      {before}
      <mark className="search-highlight">{match}</mark>
      {after}
    </>
  );
});

export default SearchHighlight;
