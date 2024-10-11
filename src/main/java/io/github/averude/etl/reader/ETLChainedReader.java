package io.github.averude.etl.reader;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An interface representing a chained reader in an ETL (Extract, Transform, Load) pipeline.
 * This reader takes an input of type {@code T} and asynchronously processes it to return a result of type {@code R}.
 *
 * @param <T> The type of the input data.
 * @param <R> The type of the result data after processing.
 */
@FunctionalInterface
public interface ETLChainedReader<T, R> {

    /**
     * Asynchronously processes the input data and returns a result.
     *
     * @param t The input data to process.
     * @return A {@link CompletableFuture} representing the result of the processing.
     */
    CompletableFuture<R> read(T t);

    /**
     * Creates an {@code ETLChainedReader} from a given synchronous function that processes
     * the input data of type {@code T} and returns a result of type {@code R}.
     * This method wraps the function in an asynchronous execution, allowing it to be used in
     * an ETL pipeline.
     *
     * @param <T> The type of the input data.
     * @param <R> The type of the result data after processing.
     * @param function The synchronous function that processes the input data and returns the result.
     * @return An instance of {@link ETLChainedReader} that asynchronously processes the input data.
     */
    static <T, R> ETLChainedReader<T, R> createReader(Function<T, R> function) {
        return (T t) -> CompletableFuture.supplyAsync(() -> function.apply(t));
    }
}
