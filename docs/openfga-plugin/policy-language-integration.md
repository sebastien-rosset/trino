# Policy Language Integration (Future Enhancement)

## Overview

The core OpenFGA plugin uses the official OpenFGA DSL for authorization model definition and the OpenFGA API for policy evaluation.

## Potential Future Enhancements

- **Cedar Integration**: Optional translation layer from Cedar policies to OpenFGA authorization model
- **OPA/Rego Integration**: Optional integration with Open Policy Agent for policy evaluation
- **Hybrid Approach**: Support multiple policy languages that compile to OpenFGA operations

These integrations would be **optional layers** built on top of the core OpenFGA functionality, not replacements for it.