import mmap, os

def set_sequential_hint(m, fd=None):
    if hasattr(os, 'posix_fadvise') and hasattr(os, 'POSIX_FADV_SEQUENTIAL'):
        if fd is None:
            fd = m.fileno()
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_SEQUENTIAL)
    if hasattr(m, 'madvise') and hasattr(mmap, 'MADV_SEQUENTIAL'):
        try:
            m.madvise(mmap.MADV_SEQUENTIAL)
        except Exception:
            print("madvise failed")
    
