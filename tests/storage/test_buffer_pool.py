from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import BufferPool, PageId
from caracaldb.storage.header import DEFAULT_PAGE_SIZE


def test_buffer_pool_fetches_pins_and_unpins_pages() -> None:
    pool = BufferPool(capacity_pages=2)

    frame = pool.fetch_page(1)
    assert len(frame.data) == DEFAULT_PAGE_SIZE
    assert frame.pin_count == 1

    pool.unpin_page(1)

    stats = pool.stats()
    assert stats.resident_pages == 1
    assert stats.pinned_pages == 0
    assert stats.misses == 1


def test_buffer_pool_hits_resident_page() -> None:
    pool = BufferPool(capacity_pages=2)

    pool.fetch_page(1)
    pool.unpin_page(1)
    pool.fetch_page(1)

    stats = pool.stats()
    assert stats.hits == 1
    assert stats.misses == 1


def test_buffer_pool_evicts_unpinned_lru_k_candidate() -> None:
    writes: dict[int, bytes] = {}

    def write_page(page_id: PageId, data: bytes) -> None:
        writes[int(page_id)] = data

    pool = BufferPool(capacity_pages=2, write_page=write_page)

    page1 = pool.new_page(1)
    page1.data[0] = 7
    pool.unpin_page(1, dirty=True)
    pool.fetch_page(2)
    pool.unpin_page(2)
    pool.fetch_page(3)

    assert not pool.contains(1)
    assert pool.contains(2)
    assert pool.contains(3)
    assert writes[1][0] == 7
    assert pool.stats().evictions == 1


def test_buffer_pool_refuses_to_evict_all_pinned_pages() -> None:
    pool = BufferPool(capacity_pages=1)
    pool.fetch_page(1)

    try:
        pool.fetch_page(2)
    except CaracalError as exc:
        assert exc.code == "CDB-7005"
    else:
        raise AssertionError("expected CaracalError")


def test_page_guard_unpins_on_exit_and_can_mark_dirty() -> None:
    writes: dict[int, bytes] = {}

    def write_page(page_id: PageId, data: bytes) -> None:
        writes[int(page_id)] = data

    pool = BufferPool(capacity_pages=1, write_page=write_page)

    with pool.guard_page(PageId(1)) as page:
        page.data[0] = 9
        page.mark_dirty()

    assert pool.stats().pinned_pages == 0
    pool.flush_all()
    assert writes[1][0] == 9
