"""Built-in function registry split by category.

The registry is shared between the binder (for name resolution) and the exec
expression compiler (for runtime dispatch). Each entry is a small record
``BuiltinFn(name, arity, kind, dispatch)`` where ``dispatch`` knows how to
turn an Arrow argument list into a result Array.
"""

from caracaldb.lang.builtins.agg import AGG_FUNCTIONS
from caracaldb.lang.builtins.graph import GRAPH_FUNCTIONS
from caracaldb.lang.builtins.scalar import SCALAR_FUNCTIONS
from caracaldb.lang.builtins.vector import VECTOR_FUNCTIONS

REGISTRY = {**SCALAR_FUNCTIONS, **AGG_FUNCTIONS, **GRAPH_FUNCTIONS, **VECTOR_FUNCTIONS}

__all__ = [
    "AGG_FUNCTIONS",
    "GRAPH_FUNCTIONS",
    "REGISTRY",
    "SCALAR_FUNCTIONS",
    "VECTOR_FUNCTIONS",
]
