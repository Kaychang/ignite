/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.future;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Future adapter.
 * TODO:
 * 1. remove serializable
 * 2. remove start time, end time.
 * 3. remove ignore interrupts flag.
 */
public class GridFutureAdapter<R> implements IgniteInternalFuture<R> {
    /*
     * https://bugs.openjdk.java.net/browse/JDK-8074773
     */
    static {
        Class<?> ensureLoaded = LockSupport.class;
    }

    /**
     * Future state.
     */
    private enum State {
        INIT,

        CANCELLED,
    }

    /**
     *
     */
    static final class WaitNode {
        /** */
        private final Object waiter;

        /** */
        private volatile Object next;

        /**
         * @param waiter Waiter.
         */
        WaitNode(Object waiter) {
            this.waiter = waiter;
        }
    }

    /** State updater. */
    private static final AtomicReferenceFieldUpdater<GridFutureAdapter, Object> stateUpd =
        AtomicReferenceFieldUpdater.newUpdater(GridFutureAdapter.class, Object.class, "state");

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private boolean ignoreInterrupts;

    /** */
    private volatile Object state = State.INIT;

    /** Future start time. */
    private final long startTime = U.currentTimeMillis();

    /** Future end time. */
    private volatile long endTime;

    /** {@inheritDoc} */
    @Override public long startTime() {
        return startTime;
    }

    /** {@inheritDoc} */
    @Override public long duration() {
        long endTime = this.endTime;

        return endTime == 0 ? U.currentTimeMillis() - startTime : endTime - startTime;
    }

    /**
     * @param ignoreInterrupts Ignore interrupts flag.
     */
    public void ignoreInterrupts(boolean ignoreInterrupts) {
        this.ignoreInterrupts = ignoreInterrupts;
    }

    /**
     * @return Future end time.
     */
    public long endTime() {
        return endTime;
    }

    /** {@inheritDoc} */
    @Override public Throwable error() {
        Object state0 = state;

        return (state0 instanceof Throwable) ? (Throwable)state0 : null;
    }

    /** {@inheritDoc} */
    @Override public R result() {
        Object state0 = state;

        return isDone(state0) && !(state0 instanceof Throwable) ? (R)state0 : null;
    }

    /** {@inheritDoc} */
    @Override public R get() throws IgniteCheckedException {
        return get0(ignoreInterrupts);
    }

    /** {@inheritDoc} */
    @Override public R getUninterruptibly() throws IgniteCheckedException {
        return get0(true);
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout) throws IgniteCheckedException {
        // Do not replace with static import, as it may not compile.
        return get(timeout, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout, TimeUnit unit) throws IgniteCheckedException {
        A.ensure(timeout >= 0, "timeout cannot be negative: " + timeout);
        A.notNull(unit, "unit");

        try {
            return get0(unit.toNanos(timeout));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException("Got interrupted while waiting for future to complete.", e);
        }
    }

    /**
     * Internal get routine.
     *
     * @param ignoreInterrupts Whether to ignore interrupts.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    private R get0(boolean ignoreInterrupts) throws IgniteCheckedException {
        Object res = registerWaiter(Thread.currentThread());

        if (res != State.INIT) {
            // no registration was done since a value is available.
            return resolve(res);
        }

        boolean interrupted = false;

        try {
            for (; ; ) {
                LockSupport.park();

                if (isDone())
                    return resolve(state);

                else if (Thread.interrupted()) {
                    interrupted = true;

                    if (!ignoreInterrupts) {
                        unregisterWaiter(Thread.currentThread());

                        throw new IgniteInterruptedCheckedException("Thread has been interrupted.");
                    }
                }
            }
        }
        finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * @param nanosTimeout Timeout (nanoseconds).
     * @return Result.
     * @throws InterruptedException If interrupted.
     * @throws IgniteFutureTimeoutCheckedException If timeout reached before computation completed.
     * @throws IgniteCheckedException If error occurred.
     */
    @Nullable protected R get0(long nanosTimeout) throws InterruptedException, IgniteCheckedException {
        Object res = registerWaiter(Thread.currentThread());

        if (res != State.INIT)
            return resolve(res);

        long deadlineNanos = System.nanoTime() + nanosTimeout;

        boolean interrupted = false;

        try {
            long nanosTimeout0 = nanosTimeout;

            while (nanosTimeout0 > 0) {
                LockSupport.parkNanos(nanosTimeout0);

                nanosTimeout0 = deadlineNanos - System.nanoTime();

                if (isDone())
                    return resolve(state);

                else if (Thread.interrupted()) {
                    interrupted = true;

                    if (!ignoreInterrupts)
                        throw new IgniteInterruptedCheckedException("Thread has been interrupted.");
                }
            }
        }
        finally {
            if (interrupted)
                Thread.currentThread().interrupt();

            unregisterWaiter(Thread.currentThread());
        }

        throw new IgniteFutureTimeoutCheckedException("Timeout was reached before computation completed.");
    }

    /**
     * Resolves the value to result or exception.
     *
     * @param val Value to resolve.
     * @return Result.
     * @throws IgniteCheckedException If resolved to exception.
     */
    private R resolve(Object val) throws IgniteCheckedException {
        if (val == State.CANCELLED)
            throw new IgniteFutureCancelledCheckedException("Future was cancelled: " + this);

        if (val instanceof Throwable)
            throw U.cast((Throwable)val);

        return (R)val;
    }

    /**
     * @param waiter Waiter to register.
     * @return Previous state.
     */
    private Object registerWaiter(Object waiter) {
        WaitNode waitNode = null;

        for (; ; ) {
            final Object oldState = state;

            if (isDone(oldState))
                return oldState;

            Object newState;

            if (oldState == State.INIT)
                newState = waiter;

            else {
                if (waitNode == null)
                    waitNode = new WaitNode(waiter);

                waitNode.next = oldState;

                newState = waitNode;
            }

            if (compareAndSetState(oldState, newState))
                return State.INIT;
        }
    }

    /**
     * @param waiter Waiter to unregister.
     */
    void unregisterWaiter(Thread waiter) {
        WaitNode prev = null;
        Object cur = state;

        while (cur != null) {
            Object curWaiter = cur.getClass() == WaitNode.class ? ((WaitNode)cur).waiter : cur;
            Object next = cur.getClass() == WaitNode.class ? ((WaitNode)cur).next : null;

            if (curWaiter == waiter) {
                if (prev == null) {
                    Object n = next == null ? State.INIT : next;

                    cur = compareAndSetState(cur, n) ? null : state;
                }
                else {
                    prev.next = next;

                    cur = null;
                }
            }
            else {
                prev = cur.getClass() == WaitNode.class ? (WaitNode)cur : null;

                cur = next;
            }
        }
    }

    /**
     * @param waiter Head of waiters queue to unblock.
     */
    private void unblockAll(Object waiter) {
        while (waiter != null) {
            if (waiter instanceof Thread) {
                LockSupport.unpark((Thread)waiter);

                return;
            }
            else if (waiter instanceof IgniteInClosure) {
                notifyListener((IgniteInClosure<? super IgniteInternalFuture<R>>)waiter);

                return;
            }
            else if (waiter.getClass() == WaitNode.class) {
                WaitNode waitNode = (WaitNode) waiter;

                unblockAll(waitNode.waiter);

                waiter = waitNode.next;
            }
            else
                return;
        }
    }

    public void unblockAllThreads() {
        unblockFirstThread0(state);
    }

    /**
     * @param waiter Head of waiters queue to unblock.
     */
    private void unblockFirstThread0(Object waiter) {
        while (waiter != null) {
            if (waiter instanceof Thread) {
                LockSupport.unpark((Thread)waiter);

                return;
            }
            else if (waiter.getClass() == WaitNode.class) {
                WaitNode waitNode = (WaitNode) waiter;

                unblockFirstThread0(waitNode.waiter);

                waiter = waitNode.next;
            }
            else
                return;
        }
    }

    /** {@inheritDoc} */
    @Override public void listen(IgniteInClosure<? super IgniteInternalFuture<R>> newLsnr) {
        Object res = registerWaiter(newLsnr);

        if (res != State.INIT)
            notifyListener(newLsnr);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<T> chain(final IgniteClosure<? super IgniteInternalFuture<R>, T> doneCb) {
        return new ChainFuture<>(this, doneCb, null);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<T> chain(final IgniteClosure<? super IgniteInternalFuture<R>, T> doneCb,
        Executor exec) {
        return new ChainFuture<>(this, doneCb, exec);
    }

    /**
     * @return Logger instance.
     */
    @Nullable public IgniteLogger logger() {
        return null;
    }

    /**
     * Notifies single listener.
     *
     * @param lsnr Listener.
     */
    private void notifyListener(IgniteInClosure<? super IgniteInternalFuture<R>> lsnr) {
        assert lsnr != null;

        try {
            lsnr.apply(this);
        }
        catch (IllegalStateException e) {
            U.error(logger(), "Failed to notify listener (is grid stopped?) [fut=" + this +
                ", lsnr=" + lsnr + ", err=" + e.getMessage() + ']', e);
        }
        catch (RuntimeException | Error e) {
            U.error(logger(), "Failed to notify listener: " + lsnr, e);

            throw e;
        }
    }

    /**
     * Default no-op implementation that always returns {@code false}.
     * Futures that do support cancellation should override this method
     * and call {@link #onCancelled()} callback explicitly if cancellation
     * indeed did happen.
     */
    @Override public boolean cancel() throws IgniteCheckedException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return isDone(state);
    }

    /**
     * @param state State to check.
     * @return {@code True} if future is done.
     */
    private boolean isDone(Object state) {
        return state == null ||
            !(state == State.INIT
                || state.getClass() == WaitNode.class
                || state instanceof Thread
                || state instanceof IgniteInClosure);
    }

    /**
     * @return Checks is future is completed with exception.
     */
    public boolean isFailed() {
        // Must read endTime first.
        return state instanceof Throwable;
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return state == State.CANCELLED;
    }

    /**
     * Callback to notify that future is finished with {@code null} result.
     * This method must delegate to {@link #onDone(Object, Throwable)} method.
     *
     * @return {@code True} if result was set by this call.
     */
    public final boolean onDone() {
        return onDone(null, null);
    }

    /**
     * Callback to notify that future is finished.
     * This method must delegate to {@link #onDone(Object, Throwable)} method.
     *
     * @param res Result.
     * @return {@code True} if result was set by this call.
     */
    public final boolean onDone(@Nullable R res) {
        return onDone(res, null);
    }

    /**
     * Callback to notify that future is finished.
     * This method must delegate to {@link #onDone(Object, Throwable)} method.
     *
     * @param err Error.
     * @return {@code True} if result was set by this call.
     */
    public final boolean onDone(@Nullable Throwable err) {
        return onDone(null, err);
    }

    /**
     * Callback to notify that future is finished. Note that if non-{@code null} exception is passed in
     * the result value will be ignored.
     *
     * @param res Optional result.
     * @param err Optional error.
     * @return {@code True} if result was set by this call.
     */
    public boolean onDone(@Nullable R res, @Nullable Throwable err) {
        return onDone(res, err, false);
    }

    /**
     * @param res Result.
     * @param err Error.
     * @param cancel {@code True} if future is being cancelled.
     * @return {@code True} if result was set by this call.
     */
    private boolean onDone(@Nullable R res, @Nullable Throwable err, boolean cancel) {
        Object val = cancel ? State.CANCELLED : err != null ? err : res;

        for (; ; ) {
            final Object oldState = state;

            if (isDone(oldState))
                return false;

            if (compareAndSetState(oldState, val)) {
                endTime = U.currentTimeMillis();

                unblockAll(oldState);

                return true;
            }
        }
    }

    /**
     * @param oldState Old state to check.
     * @param newState New state to set.
     * @return {@code True} if state has been changed (CASed).
     */
    private boolean compareAndSetState(Object oldState, Object newState) {
        return state == oldState && stateUpd.compareAndSet(this, oldState, newState);
    }

    /**
     * Callback to notify that future is cancelled.
     *
     * @return {@code True} if cancel flag was set by this call.
     */
    public boolean onCancelled() {
        return onDone(null, null, true);
    }

    /**
     * @return String representation of state.
     */
    private String state() {
        Object s = state;

        return s instanceof State ? s.toString() : isDone(s) ? "DONE" : "INIT";
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridFutureAdapter.class, this, "state", state(), "hash", System.identityHashCode(this));
    }

    /**
     *
     */
    private static class ChainFuture<R, T> extends GridFutureAdapter<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private GridFutureAdapter<R> fut;

        /** */
        private IgniteClosure<? super IgniteInternalFuture<R>, T> doneCb;

        /**
         *
         */
        public ChainFuture() {
            // No-op.
        }

        /**
         * @param fut Future.
         * @param doneCb Closure.
         * @param cbExec Optional executor to run callback.
         */
        ChainFuture(
            GridFutureAdapter<R> fut,
            IgniteClosure<? super IgniteInternalFuture<R>, T> doneCb,
            @Nullable Executor cbExec
        ) {
            this.fut = fut;
            this.doneCb = doneCb;

            fut.listen(new GridFutureChainListener<>(this, doneCb, cbExec));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "ChainFuture [orig=" + fut + ", doneCb=" + doneCb + ']';
        }
    }
}
