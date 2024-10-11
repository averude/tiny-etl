package io.github.averude.etl.writer;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Functional interface for writing data in the ETL (Extract, Transform, Load) process.
 *
 * @param <T> The type of data to write.
 */
@FunctionalInterface
public interface ETLWriter<T> {

    /**
     * Writes the specified data asynchronously.
     * <p>
     * This method initiates an asynchronous operation to write the provided data.
     * The result of the write operation is encapsulated in a CompletableFuture,
     * allowing for non-blocking execution and easy composition of asynchronous tasks.
     *
     * @param t The data to write.
     * @return A CompletableFuture representing the result of the write operation.
     */
    CompletableFuture<T> write(T t);

    /**
     * Creates an ETLWriter that uses a specified consumer for the write operation.
     * <p>
     * This static method allows users to create an instance of ETLWriter using a Consumer.
     * The provided consumer handles the writing of data asynchronously.
     *
     * @param <T>      The type of data to write.
     * @param consumer The consumer that handles the writing operation.
     * @return A new ETLWriter instance that writes data using the provided consumer.
     */
    static <T> ETLWriter<T> createWriter(Consumer<T> consumer) {
        return (T value) -> CompletableFuture
                .runAsync(() -> consumer.accept(value))
                .thenApply((unused) -> value);
    }

    /**
     * Creates an ETLWriter that uses a specified function for the write operation.
     * <p>
     * This static method allows users to create an instance of ETLWriter using a Function.
     * The provided function processes the data before writing it asynchronously.
     *
     * @param <T>      The type of data to write.
     * @param consumer The function that processes the data before writing.
     * @return A new ETLWriter instance that writes data using the provided function.
     */
    static <T> ETLWriter<T> createWriter(Function<T, T> consumer) {
        return (T value) -> CompletableFuture
                .supplyAsync(() -> consumer.apply(value));
    }
}

