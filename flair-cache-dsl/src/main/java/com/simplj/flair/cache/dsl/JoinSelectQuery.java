package com.simplj.flair.cache.dsl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Terminal stage of a join chain; carries the type-safe output projection.
 * Created by {@link JoinQuery#select}; call {@link #fetch()} to execute the join.
 *
 * <p>Because the projection type {@code S} is a type parameter of this class,
 * {@link #fetch()} returns {@code List<S>} without any unchecked cast.</p>
 */
public final class JoinSelectQuery<L, R, S> {

    private final Collection<Object>  leftRaw;
    private final Decoder<L>          leftDecoder;
    private final Collection<Object>  rightRaw;
    private final Decoder<R>          rightDecoder;
    private final Predicate<L>        leftFilter;
    private final BiPredicate<L, R>   joinCondition;  // null when hash-join strategy was chosen
    private final Function<L, Object> leftKeyFn;      // null when nested-loop strategy was chosen
    private final Function<R, Object> rightKeyFn;
    private final BiFunction<L, R, S> selector;

    JoinSelectQuery(Collection<Object> leftRaw,  Decoder<L> leftDecoder,
                    Collection<Object> rightRaw, Decoder<R> rightDecoder,
                    Predicate<L>        leftFilter,
                    BiPredicate<L, R>   joinCondition,
                    Function<L, Object> leftKeyFn,
                    Function<R, Object> rightKeyFn,
                    BiFunction<L, R, S> selector) {
        this.leftRaw      = leftRaw;
        this.leftDecoder  = leftDecoder;
        this.rightRaw     = rightRaw;
        this.rightDecoder = rightDecoder;
        this.leftFilter   = leftFilter;
        this.joinCondition = joinCondition;
        this.leftKeyFn    = leftKeyFn;
        this.rightKeyFn   = rightKeyFn;
        this.selector     = selector;
    }

    /**
     * Executes the join and returns matched, projected results.
     *
     * @throws IllegalStateException if {@link JoinQuery#on} was not called before
     *         {@link JoinQuery#select}
     */
    public List<S> fetch() {
        if (joinCondition == null && leftKeyFn == null) {
            throw new IllegalStateException("on() must be called before fetch()");
        }
        List<L> left  = decodeAll(leftRaw,  leftDecoder);
        List<R> right = decodeAll(rightRaw, rightDecoder);
        if (leftKeyFn != null) {
            return JoinEngine.hashJoin(left, right, leftKeyFn, rightKeyFn, leftFilter, selector);
        }
        return JoinEngine.nestedLoopJoin(left, right, joinCondition, leftFilter, selector);
    }

    private static <T> List<T> decodeAll(Collection<Object> raw, Decoder<T> decoder) {
        List<T> list = new ArrayList<>(raw.size());
        for (Object item : raw) list.add(decoder.decode(item));
        return list;
    }
}
