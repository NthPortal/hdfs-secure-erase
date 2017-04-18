package tmp.namespace.undecided.core.specs;

import tmp.namespace.undecided.core.FileErasureSpec;

/**
 * Abstract specification for deleting files.
 */
abstract class FileDeletionSpec extends FileErasureSpec {
    @Override
    public final boolean isTerminal() {
        return true;
    }
}
