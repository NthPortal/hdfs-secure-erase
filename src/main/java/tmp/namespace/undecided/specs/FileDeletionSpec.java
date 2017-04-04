package tmp.namespace.undecided.specs;

import tmp.namespace.undecided.FileErasureSpec;

abstract class FileDeletionSpec extends FileErasureSpec {
    @Override
    public final boolean isTerminal() {
        return true;
    }
}
