package io.github.averude.etl.util;

import io.github.averude.etl.reader.ETLChainedReader;
import io.github.averude.etl.reader.ETLReader;
import io.github.averude.etl.writer.ETLWriter;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * Utility class for combining multiple ETLReaders and ETLWriters.
 */
public final class ETLCombiners {

    private ETLCombiners() {}

    /**
     * Combines two ETLReaders sequentially, applying a combination function to their results.
     *
     * @param <T1>            The type of data read from the first reader.
     * @param <T2>            The type of data read from the second reader.
     * @param <R>             The result type after combining both data.
     * @param reader1         The first ETLReader.
     * @param reader2         The second ETLReader.
     * @param combineFunction The function to combine both results.
     * @return A new ETLReader that combines the results of both readers.
     */
    public static <T1, T2, R> ETLReader<R> combine(ETLReader<T1> reader1,
                                                   ETLReader<T2> reader2,
                                                   BiFunction<T1, T2, R> combineFunction) {
        return () -> reader1.read().
                thenCompose(t1 -> reader2.read()
                        .thenApply(t2 -> combineFunction.apply(t1, t2)));
    }

    /**
     * Combines two ETLReaders in parallel, applying a combination function to their results.
     *
     * @param <T1>            The type of data read from the first reader.
     * @param <T2>            The type of data read from the second reader.
     * @param <R>             The result type after combining both data.
     * @param reader1         The first ETLReader.
     * @param reader2         The second ETLReader.
     * @param combineFunction The function to combine both results.
     * @return A new ETLReader that combines the results of both readers in parallel.
     */
    public static <T1, T2, R> ETLReader<R> combineParallel(ETLReader<T1> reader1,
                                                           ETLReader<T2> reader2,
                                                           BiFunction<T1, T2, R> combineFunction) {
        return () -> reader1.read()
                .thenCombineAsync(reader2.read(), combineFunction);
    }

    /**
     * Combines two ETLChainedReaders sequentially, applying a combination function to their results.
     *
     * @param <T>             The input type of the chained readers.
     * @param <R1>            The result type of the first chained reader.
     * @param <R2>            The result type of the second chained reader.
     * @param <R>             The final result type after combining the results.
     * @param reader1         The first ETLChainedReader.
     * @param reader2         The second ETLChainedReader.
     * @param combineFunction The function to combine the results of both readers.
     * @return A new ETLChainedReader that combines the results of both chained readers.
     */
    public static <T, R1, R2, R> ETLChainedReader<T, R> combine(ETLChainedReader<T, R1> reader1,
                                                                ETLChainedReader<T, R2> reader2,
                                                                BiFunction<R1, R2, R> combineFunction) {
        return (T t) -> reader1.read(t)
                .thenCompose(r1 -> reader2.read(t)
                        .thenApply(r2 -> combineFunction.apply(r1, r2)));
    }

    /**
     * Combines two ETLChainedReaders in parallel, applying a combination function to their results.
     *
     * @param <T>             The input type of the chained readers.
     * @param <R1>            The result type of the first chained reader.
     * @param <R2>            The result type of the second chained reader.
     * @param <R>             The final result type after combining the results.
     * @param reader1         The first ETLChainedReader.
     * @param reader2         The second ETLChainedReader.
     * @param combineFunction The function to combine the results of both readers.
     * @return A new ETLChainedReader that combines the results of both chained readers in parallel.
     */
    public static <T, R1, R2, R> ETLChainedReader<T, R> combineParallel(ETLChainedReader<T, R1> reader1,
                                                                        ETLChainedReader<T, R2> reader2,
                                                                        BiFunction<R1, R2, R> combineFunction) {
        return (T t) -> reader1.read(t)
                .thenCombineAsync(reader2.read(t), combineFunction);
    }

    /**
     * Combines two ETLWriters sequentially, writing the same data to both.
     *
     * @param <T>     The type of data to write.
     * @param writer1 The first ETLWriter.
     * @param writer2 The second ETLWriter.
     * @return A new ETLWriter that writes data to both writers sequentially.
     */
    public static <T> ETLWriter<T> combine(ETLWriter<T> writer1,
                                           ETLWriter<T> writer2) {
        return (value) -> writer1.write(value)
                .thenCompose(v1 -> writer2.write(value));
    }

    /**
     * Combines multiple ETLWriters sequentially, writing the same data to each writer in the order they are provided.
     *
     * <p>This method takes a variable number of ETLWriters and ensures that the data is written to each writer
     * one after the other. The result of each write operation is chained, meaning that the next write will not
     * begin until the previous one has completed.</p>
     *
     * @param <T>     The type of data to write.
     * @param writers  The ETLWriters to combine. At least two writers must be provided.
     * @return A new ETLWriter that writes data to all specified writers in sequence.
     * @throws IllegalArgumentException if fewer than two writers are provided.
     */
    @SafeVarargs
    public static <T> ETLWriter<T> combine(ETLWriter<T> ... writers) {
        if (writers.length < 2) {
            throw new IllegalArgumentException("At least two writers are required");
        }

        return (value) -> {
            var completableFuture = writers[0].write(value);

            for (int i = 1; i < writers.length; i++) {
                var nextWriter = writers[i];
                completableFuture = completableFuture
                        .thenCompose(v1 -> nextWriter.write(value));
            }

            return completableFuture;
        };
    }

    /**
     * Combines two ETLWriters in parallel, writing the same data to both.
     *
     * @param <T>     The type of data to write.
     * @param writer1 The first ETLWriter.
     * @param writer2 The second ETLWriter.
     * @return A new ETLWriter that writes data to both writers in parallel.
     */
    public static <T> ETLWriter<T> combineParallel(ETLWriter<T> writer1,
                                                   ETLWriter<T> writer2) {
        return (value) -> CompletableFuture.allOf(writer1.write(value), writer2.write(value))
                .thenApply(unused -> value);
    }

    /**
     * Combines multiple ETLWriters in parallel, writing the same data to all.
     *
     * @param <T>     The type of data to write.
     * @param writers  The ETLWriters to combine.
     * @return A new ETLWriter that writes data to all writers in parallel.
     * @throws IllegalArgumentException if fewer than two writers are provided.
     */
    @SafeVarargs
    public static <T> ETLWriter<T> combineParallel(ETLWriter<T> ... writers) {
        if (writers.length < 2) {
            throw new IllegalArgumentException("At least two writers are required");
        }

        return (value) -> CompletableFuture
                .allOf(Stream.of(writers)
                        .map(writer -> writer.write(value))
                        .toArray(CompletableFuture[]::new))
                .thenApply(unused -> value);
    }
}
