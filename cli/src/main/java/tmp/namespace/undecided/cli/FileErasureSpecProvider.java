package tmp.namespace.undecided.cli;

import tmp.namespace.undecided.core.FileErasureSpec;

/**
 * Provides an {@link FileErasureSpec}.
 */
public interface FileErasureSpecProvider {
    /**
     * Returns an {@link FileErasureSpec}.
     *
     * @return an ErasureSpec
     */
    FileErasureSpec get();
}
