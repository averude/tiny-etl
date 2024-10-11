package io.github.averude.etl.reader;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Functional interface for reading data in the ETL (Extract, Transform, Load) process.
 *
 * @param <T> The type of data to read.
 */
@FunctionalInterface
public interface ETLReader<T> {
    /**
     * Reads the data asynchronously.
     * <p>
     * This method initiates an asynchronous operation to read data.
     * The result of the read operation is encapsulated in a CompletableFuture,
     * allowing for non-blocking execution and easy composition of asynchronous tasks.
     *
     * @return A CompletableFuture representing the result of the read operation.
     */
    CompletableFuture<T> read();

    /**
     * Creates an ETLReader that uses a specified supplier for the read operation.
     * <p>
     * This static method allows users to create an instance of ETLReader using a Supplier.
     * The provided supplier is used to obtain data asynchronously.
     *
     * @param <T>      The type of data to read.
     * @param supplier The supplier that provides the data to read.
     * @return A new ETLReader instance that reads data using the provided supplier.
     */
    static <T> ETLReader<T> createReader(Supplier<T> supplier) {
        return () -> CompletableFuture.supplyAsync(supplier);
    }

    /**
     * Creates an ETLReader that performs a sequential read operation, where the
     * result of the first read is passed to the second read operation.
     * <p>
     * This static method allows for chaining read operations. It first reads data
     * using the first supplier, then applies the second function to transform
     * that data into a different type.
     *
     * @param <T>      The type of data read from the first operation.
     * @param <R>      The type of data produced by the second operation.
     * @param firstRead A supplier for the first read operation.
     * @param secondRead A function that transforms the result of the first read.
     * @return A new ETLReader instance that reads data sequentially.
     */
    static <T, R> ETLReader<R> createSequentialReader(Supplier<T> firstRead,
                                                      Function<T, R> secondRead) {
        return () -> CompletableFuture.supplyAsync(firstRead).thenApply(secondRead);
    }

    /**
     * Creates an ETLReader that performs a sequential read operation, allowing
     * for the results of both the first and second reads to be merged into a final result.
     * <p>
     * This static method allows for a more complex read operation where two suppliers
     * are executed in sequence. The first supplier's result is passed to the second,
     * and the results of both suppliers are merged using the provided BiFunction.
     *
     * @param <T1>               The type of data read from the first operation.
     * @param <T2>               The type of data read from the second operation.
     * @param <R>                The type of the merged result.
     * @param firstRead          A supplier for the first read operation.
     * @param secondRead         A function that transforms the result of the first read into a second type.
     * @param resultMergeFunction A BiFunction that merges the results of both read operations.
     * @return A new ETLReader instance that reads and merges data sequentially.
     */
    static <T1, T2, R> ETLReader<R> createSequentialReader(Supplier<T1> firstRead,
                                                           Function<T1, T2> secondRead,
                                                           BiFunction<T1, T2, R> resultMergeFunction) {
        return () -> CompletableFuture.supplyAsync(firstRead)
                .thenCompose(t1 -> CompletableFuture.supplyAsync(() -> secondRead.apply(t1))
                        .thenApply(t2 -> resultMergeFunction.apply(t1, t2)));
    }
}

