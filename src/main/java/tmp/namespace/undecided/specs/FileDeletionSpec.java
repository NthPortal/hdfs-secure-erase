package tmp.namespace.undecided.specs;

import tmp.namespace.undecided.FileErasureSpec;

/**
 * Abstract specification for deleting files.
 */
abstract class FileDeletionSpec extends FileErasureSpec {
    @Override
    public final boolean isTerminal() {
        return true;
    }
}
