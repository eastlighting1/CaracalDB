---
applies_to: v0.2.x
status: experimental
last_updated: 2026-04-30
engine_status: python-reference; rust-engine-planned
---

# Ontology

CaracalDB uses ontology metadata to keep graph names meaningful. Classes and properties are not just labels; they can participate in hierarchy, domain, range, and closure rules that make query behavior more predictable across datasets.

## Support Level

| Surface | Status in v0.2.x |
|---|---|
| Class registration with local names | Executable |
| Class IRIs and superclass metadata | Executable |
| `alias.class SUBCLASSOF* <IRI>` | Executable in the focused query path |
| Property closure | Experimental |
| `INFER CLOSURE` materialization | Parsed utility syntax, not a stable execution surface |

## Mental Model

```mermaid
flowchart TD
    A["Ontology"] --> B["Class hierarchy"]
    A --> C["Property hierarchy"]
    B --> D["SUBCLASSOF closure"]
    C --> E["SUBPROPERTYOF closure"]
    D --> F["Bitmap or indexed membership"]
    E --> F
    F --> G["Tuft predicates and reasoning"]
```
## Core Ideas

| Concept | Meaning |
|---|---|
| Ontology | A named model that groups class and property definitions. |
| Class hierarchy | Parent/child relationships between node classes. |
| Property hierarchy | Parent/child relationships between edge or datatype properties. |
| Closure | The transitive result of hierarchy rules. |
| Domain | Which class a property can start from. |
| Range | Which class or datatype a property can point to. |

## Catalog Shape

For executable v0.1.x code, define classes through the database handle:

```python
import caracaldb as cdb
from pathlib import Path
from tempfile import TemporaryDirectory

with TemporaryDirectory() as tmp:
    path = Path(tmp) / "ontology-demo.crcl"
    with cdb.connect(path) as db:
        db.define_class("Gene", iri="http://example.org/Gene")
        db.define_class(
            "ProteinCodingGene",
            iri="http://example.org/ProteinCodingGene",
            superclass_iris=("http://example.org/Gene",),
        )
        print([cls.local_name for cls in db.catalog.classes])
```

Expected output:

```text
['Gene', 'ProteinCodingGene']
```

The lower-level catalog stores the hierarchy metadata that closure-aware reads consume:

```python
from caracaldb.onto.catalog import Catalog

catalog = Catalog.empty()
catalog.register_class(iri="http://example.org/Gene", local_name="Gene")
catalog.register_class(
    iri="http://example.org/ProteinCodingGene",
    local_name="ProteinCodingGene",
    superclass_iris=("http://example.org/Gene",),
)
print([(cls.local_name, cls.superclass_iris) for cls in catalog.classes])
```

Expected output:

```text
[('Gene', ()), ('ProteinCodingGene', ('http://example.org/Gene',))]
```
The superclass link is data, not prose. That lets documentation, validation, query binding, and closure indexes read from the same model.

## Query Shape

Tuft supports the focused class hierarchy predicate in the current MVP query path. This executable example inserts a subclass node and reads it through the superclass closure predicate:

```python
import caracaldb as cdb
from pathlib import Path
from tempfile import TemporaryDirectory

with TemporaryDirectory() as tmp:
    path = Path(tmp) / "ontology-query.crcl"
    with cdb.connect(path) as db:
        db.define_class("Gene", iri="http://example.org/Gene")
        db.define_class(
            "ProteinCodingGene",
            iri="http://example.org/ProteinCodingGene",
            superclass_iris=("http://example.org/Gene",),
        )
        db.insert_nodes("ProteinCodingGene", [{"symbol": "TP53"}])

        rows = db.sql("""
        MATCH (g:ProteinCodingGene)
        WHERE g.class SUBCLASSOF* <http://example.org/Gene>
        RETURN g.symbol
        """).rows()
        print(rows)
```

Expected output:

```text
[{'symbol': 'TP53'}]
```
The `*` means transitive closure: direct subclasses and indirect subclasses can both match the requested superclass.

!!! note "Common misconception"
    Ontology support is not the same thing as importing every OWL feature. CaracalDB focuses on the subset that can be made explicit, testable, and useful for graph queries and ML pipelines.
