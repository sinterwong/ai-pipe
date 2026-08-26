# Comment style

Comments record information that the code cannot express clearly. They explain
contracts, constraints, and intent; they do not narrate the implementation.

This guide applies to first-party C++ code under `include/`, `src/`, `tests/`,
`benchmarks/`, and `examples/`. Do not reformat vendored code under `3rdparty/`
to match it.

## Language and tone

- Write comments in English.
- Be direct and specific. Prefer facts over introductions such as "Note that"
  or "This function is responsible for".
- Use complete sentences for API documentation and multi-line explanations.
  Short labels may be sentence fragments.
- Refer to identifiers with backticks. Use the same domain terms as the public
  API and documentation.
- Keep comments close to the code whose behavior they constrain.
- When touching existing code, improve comments relevant to the change. Do not
  rewrite unrelated legacy comments solely to make them conform to this guide.

## What to document

Write a comment when it answers at least one of these questions:

- Why is the implementation shaped this way?
- Which precondition or invariant is not encoded by the type system?
- What are the port name, packet payload type, frame identity, time unit, queue
  bound, or ordering semantics?
- Who owns the memory, how long is it valid, and when is work synchronized?
- Which scheduling, synchronization, backpressure, plugin ABI, or compatibility
  behavior must remain stable?
- What contract is a test protecting, and why is its tolerance appropriate?

Do not comment code that already states the same fact:

```cpp
// Increment the index.
++index;
```

Name the consequence when preserving an unusual choice:

```cpp
// `std_vals` is a divisor: 255 scales 8-bit pixels to [0, 1].
arg.std_vals = {255.0F, 255.0F, 255.0F};
```

## Public API

Use Doxygen comments for public declarations under `include/ai_pipe/`. Follow
the existing style: `/** ... */` for types and functions, and `///<` for a short
field or enumerator note. Document the observable contract rather than the
implementation:

- accepted node/port names, packet payload types, frame IDs, stream IDs,
  timestamps, and time units;
- packet, graph, context, callback, and returned-view ownership/lifetime;
- synchronous or asynchronous completion, cancellation, and end-of-stream
  behavior;
- thread-safety and execution-context ownership;
- errors, invalid states, and non-obvious side effects.

Do not add boilerplate `@param` or `@return` entries when the signature and the
summary already make them clear. Simple accessors need no comment unless they
have unusual lifetime or state semantics. Use Doxygen commands such as
`@param`, `@return`, `@throws`, and `@par Thread safety` when they add contract
information that is not obvious from the declaration.

Do not add metadata-only file banners. A file-level comment is useful when it
explains the role or constraints of the whole file, not merely its filename,
author, date, or version.

## Internal implementation

Use `//` in implementation files. Comments are most useful around:

- graph compilation, topology indexing, and edge routing;
- queue ownership, lock-free memory ordering, capacity, and drop behavior;
- scheduling, frame alignment, synchronization, and end-of-stream propagation;
- plugin discovery, registration, unloading, and ABI behavior;
- compatibility behavior that looks accidental;
- non-obvious performance tradeoffs.

Avoid a header comment for every private function. Prefer a short explanation
at the decision or expression it applies to.

## Tests

Test names should describe the behavior. Add comments only for information the
name and assertions cannot convey, such as:

- why a fixture uses a boundary-sized or deliberately misaligned input;
- why timing, concurrency, or statistics comparisons are approximate;
- which historical regression or pipeline contract is covered;
- why an apparently redundant assertion protects a separate contract.

Large test files may use short Markdown-style section headings. Do not use rows
of punctuation as visual separators.

## TODO comments

A TODO must state a concrete remaining action and include an issue reference
when one exists:

```cpp
// TODO(#123): Reject tensors whose shape does not match the model metadata.
```

Do not use TODOs for vague design wishes. Put larger design work in an issue or
under `docs/` and link to it from the relevant code if necessary.

## Longer explanations

Keep pipeline architecture, synchronization protocols, plugin integration,
migration history, and benchmark or tuning rationale under `docs/`. Source
comments should state the local constraint and link to the document when the
background is needed to change it safely.

## Review checklist

- Could a clearer name or type remove the need for this comment?
- Does the comment explain a contract or reason instead of restating code?
- Are port names, payload types, frame identity, ownership, and synchronization
  explicit where needed?
- Does a compatibility comment identify the observable consequence?
- Is the comment still true on every control-flow path it describes?
- Is unrelated code absent from the comment-only change?

## Project terminology

Use these terms consistently:

| Prefer | Avoid or qualify |
| --- | --- |
| pipeline, execution engine | framework when the specific layer matters |
| node, source node, sink node | handler, worker, processor without its graph role |
| input port, output port | input/output endpoint when referring to a node port |
| packet, payload | data when ownership or type matters |
| frame ID, stream ID, timestamp | frame metadata without naming the field |
| compiled graph, topology snapshot | cached graph without naming the representation |
| scheduler strategy, synchronization strategy | strategy without its role |
| queue capacity, queue depth | queue size when the distinction matters |
| drop strategy, dropped frame | discard policy, lost data |
| end of stream | EOS before the term has been introduced |
| plugin | component when dynamic registration or loading matters |
| owns storage, shared ownership, immutable view | holds a pointer, borrowed data |
| synchronous, asynchronous | sync, async in explanatory prose |
