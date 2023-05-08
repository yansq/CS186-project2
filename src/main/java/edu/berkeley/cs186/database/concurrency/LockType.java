package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    // shared
    S,
    // exclusive
    X,
    // intention shared
    IS,
    // intention exclusive
    IX,
    // shared intention exclusive
    SIX,
    // no lock held
    NL;

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        switch (a) {
            case NL:
                return true;
            case IS:
                return b != LockType.X;
            case IX:
                return b == LockType.NL || b == LockType.IS || b == LockType.IX;
            case S:
                return b == LockType.NL || b == LockType.IS || b == LockType.S;
            case SIX:
                return b == LockType.NL || b == LockType.IS;
            case X:
                return b == LockType.NL;
            default:
                throw new NullPointerException("bad lock type");
        }
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        switch (parentLockType) {
            case NL:
            case S:
            case X:
                return childLockType == LockType.NL;
            case IS:
                return childLockType == LockType.NL || childLockType == LockType.S || childLockType == LockType.IS;
            case IX:
                return true;
            case SIX:
                return childLockType == LockType.NL || childLockType == LockType.X || childLockType == LockType.IX
                        || childLockType == LockType.SIX;
            default:
                throw new NullPointerException("bad lock type");
        }
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        switch (substitute) {
            case NL:
                return required == LockType.NL;
            case IS:
                return required == LockType.NL || required == LockType.IS;
            case IX:
                return required == LockType.NL || required == LockType.IS || required == LockType.IX;
            case S:
                return required == LockType.NL || required == LockType.IS || required == LockType.S;
            case SIX:
                return required == LockType.NL || required == LockType.IS || required == LockType.S
                        || required == LockType.IX;
            case X:
                return true;
            default:
                throw new NullPointerException("bad lock type");
        }
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

