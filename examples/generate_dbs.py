import shutil
from pathlib import Path

import pyarrow as pa
import caracaldb as cdb

from caracaldb.onto.catalog import Catalog, save_catalog
from caracaldb.storage.node_store import open_node_store

def create_simple(out_path: Path):
    print(f"Creating {out_path}...")
    with cdb.connect(out_path) as db:
        catalog = Catalog.empty()
        catalog.register_class(iri="http://example.org/Person", local_name="Person")
        save_catalog(db.bundle, catalog)

        store = open_node_store(db.bundle, class_iri="http://example.org/Person", local_name="Person", create=True)
        store.append(
            pa.record_batch(
                {
                    "name": pa.array(["Alice", "Bob", "Charlie", "Diana"]),
                    "age": pa.array([28, 34, 25, 42]),
                    "city": pa.array(["New York", "London", "Paris", "Tokyo"]),
                }
            )
        )

def create_weighted(out_path: Path):
    print(f"Creating {out_path}...")
    with cdb.connect(out_path) as db:
        catalog = Catalog.empty()
        catalog.register_class(iri="http://example.org/GraphNode", local_name="GraphNode")
        save_catalog(db.bundle, catalog)

        store = open_node_store(db.bundle, class_iri="http://example.org/GraphNode", local_name="GraphNode", create=True)
        store.append(
            pa.record_batch(
                {
                    "node_id": pa.array(["n1", "n2", "n3", "n4", "n5"]),
                    "pagerank": pa.array([0.15, 0.85, 1.2, 0.45, 0.99]),
                    "degree": pa.array([2, 10, 15, 3, 8]),
                    "weight": pa.array([1.0, 0.5, 0.25, 0.75, 1.5]),
                }
            )
        )

def create_complex(out_path: Path):
    print(f"Creating {out_path}...")
    with cdb.connect(out_path) as db:
        catalog = Catalog.empty()
        catalog.register_class(iri="http://example.org/Movie", local_name="Movie")
        catalog.register_class(iri="http://example.org/Actor", local_name="Actor")
        save_catalog(db.bundle, catalog)

        movie_store = open_node_store(db.bundle, class_iri="http://example.org/Movie", local_name="Movie", create=True)
        movie_store.append(
            pa.record_batch(
                {
                    "title": pa.array(["Inception", "The Matrix", "Interstellar"]),
                    "year": pa.array([2010, 1999, 2014]),
                    "rating": pa.array([8.8, 8.7, 8.6]),
                }
            )
        )

        actor_store = open_node_store(db.bundle, class_iri="http://example.org/Actor", local_name="Actor", create=True)
        actor_store.append(
            pa.record_batch(
                {
                    "name": pa.array(["Leonardo DiCaprio", "Keanu Reeves", "Matthew McConaughey"]),
                    "birth_year": pa.array([1974, 1964, 1969]),
                }
            )
        )

def main():
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    create_simple(data_dir / "example_simple.crcl")
    create_weighted(data_dir / "example_weighted.crcl")
    create_complex(data_dir / "example_complex.crcl")
    print("Done generating sample DBs.")

if __name__ == "__main__":
    main()
