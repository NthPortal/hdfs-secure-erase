package tmp.namespace.undecided;

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
