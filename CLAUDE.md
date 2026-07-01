## Code navigation policy

When working in this repository, prefer **LSP / symbol-aware tools** over raw text search.

### Required behavior
1. Use **LSP-based queries first** for symbol-aware tasks:
   - go to definition
   - find references
   - find implementations
   - symbol lookup
   - rename impact analysis
   - diagnostics and type errors
   - understanding call sites, interfaces, and type relationships

2. Use `rg`, `grep`, or `find` only when:
   - searching for literal strings, comments, or docs
   - locating filenames, generated files, or config files
   - the LSP cannot answer the question
   - confirming broad text usage after symbol-aware results are exhausted


### Decision rule
- **Symbol / type / reference question** -> use LSP first
- **Literal string / comment / docs / filename question** -> use text search
- **Unsure** -> try LSP first, then fall back to text search only if needed
