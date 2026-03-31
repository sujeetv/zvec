# CLAUDE.md — zvec

## Project Overview

zvec is a high-performance, in-process vector database built on Alibaba's Proxima technology. C++17 core with Python bindings via pybind11.

## Build

```bash
pip install -e ".[dev]"          # Editable install with dev deps
pytest python/tests/ -v          # Run Python tests
```

For C++ only:
```bash
mkdir build && cd build
cmake .. -DBUILD_PYTHON_BINDINGS=ON -G Ninja
cmake --build .
cmake --build . --target unittest  # Run C++ tests
```

## Code Style

### C++

- **Format:** Google style via clang-format (`.clang-format` in repo root)
- **Headers:** `#pragma once`, not `#ifndef` guards
- **Classes:** `PascalCase` (e.g., `HnswBuilder`, `GraphEngine`)
- **Methods:** `snake_case` (e.g., `do_build()`, `init()`)
- **Private members:** `snake_case_` with trailing underscore (e.g., `thread_cnt_`, `schema_`)
- **Constants:** `SCREAMING_SNAKE_CASE` for globals, `kPrefixCase` for class constants (e.g., `kDefaultLogIntervalSecs`)
- **Enums (protobuf):** `SCREAMING_SNAKE_CASE` (e.g., `DT_STRING`, `IT_HNSW`)
- **Namespaces:** Nested with closing comment: `}  // namespace zvec`
- **Docs:** `//!` comment style above methods
- **Smart pointers:** Use `::Pointer` / `::UPointer` type aliases
- **Standard:** C++17

### Python

- **First line:** `from __future__ import annotations`
- **Format:** ruff format, line length 88, double quotes
- **Imports:** `__future__` > stdlib > third-party > local (enforced by ruff isort)
- **Classes:** `PascalCase`, functions/methods `snake_case`
- **Type hints:** Required on all function signatures
- **Docstrings:** Google style
- **Data classes:** Use `__slots__` for memory efficiency
- **Exports:** Use `__all__` in every public module
- **Linting:** ruff with extended checks (B, I, ARG, C4, PT, UP, NPY, etc.)

### Git

- **Branches:** `<type>/<kebab-case>` (e.g., `feat/graph-engine`, `fix/traversal-bug`)
- **Commits:** Conventional format: `type(scope): description`
  - Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`
- **Pre-commit hooks:** clang-format, ruff lint+format, gitleaks (secret scanning)
- **GitHub issues/PRs:** Always create on the fork `sujeetv/zvec`, never on upstream `alibaba/zvec`

## Architecture

```
src/db/              # Core database: collections, segments, RocksDB storage
src/core/            # Index algorithms: HNSW, IVF, FLAT, RaBitQ, sparse
src/ailego/          # Utility library: threading, logging, math, IO
src/binding/python/  # Pybind11 bindings (_zvec module)
src/graph/           # Property graph engine (new)
python/zvec/         # Python API layer
python/tests/        # Pytest test suite
```

## Key Patterns

### Adding a C++ module

1. Create `src/mymodule/CMakeLists.txt` using `cc_library()`
2. Add `cc_directory(mymodule)` to `src/CMakeLists.txt`
3. Tests go in `tests/mymodule/` using `cc_gtest()`
4. Add `cc_directory(mymodule)` to `tests/CMakeLists.txt`

### Adding pybind11 bindings

1. Header in `src/binding/python/include/python_mymodule.h`
2. Implementation in `src/binding/python/model/python_mymodule.cc`
3. Register `Initialize(m)` in `src/binding/python/binding.cc`
4. Add source to `SRC_LISTS` in `src/binding/python/CMakeLists.txt`

### Pybind11 pattern

```cpp
class ZVecPyMyModule {
 public:
  ZVecPyMyModule() = delete;
  static void Initialize(py::module_& m);
};
```

### Python wrapper pattern

- C++ classes exposed as `_ClassName` (e.g., `_Collection`, `_GraphEngine`)
- Python wrappers in `python/zvec/` hold `self._cpp_obj` and delegate
- Convert errors with `unwrap_expected()` pattern

## Copyright Header

Required on every file:
```
Copyright 2025-present the zvec project
Licensed under the Apache License, Version 2.0
```
