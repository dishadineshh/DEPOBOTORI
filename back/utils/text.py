def chunk_stream(text: str, size: int = 900, overlap: int = 120):
    """Yield chunks one-by-one (memory-safe)."""
    n = len(text)
    if n == 0:
        return
    i = 0
    prev = -1
    while i < n:
        end = i + size
        if end > n: end = n
        yield text[i:end]
        if end >= n: break
        i = end - overlap
        if i <= prev: i = prev + 1
        prev = i
