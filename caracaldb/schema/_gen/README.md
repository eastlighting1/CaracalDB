# Generated FlatBuffers Bindings

This directory is reserved for Python files generated from `schema/catalog.fbs`.

Expected command once generated bindings are needed by runtime code:

```bash
flatc --python --gen-object-api -o caracaldb/schema/_gen schema/catalog.fbs
```

CI installs `flatc` and validates that `schema/catalog.fbs` compiles. Generated
files are intentionally not hand-written or committed until the runtime switches
from the temporary JSON catalog adapter to FlatBuffers object bindings.
