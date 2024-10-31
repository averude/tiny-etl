package io.github.averude.etl.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Functional interface for writing data in the ETL (Extract, Transform, Load) process.
 *
 * @param <T> The type of data to write.
 */
@FunctionalInterface
public interface ETLWriter<T> {

    Logger LOG = LoggerFactory.getLogger(ETLWriter.class);
    AtomicLong WRITERS_COUNT = new AtomicLong(1);

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
        Objects.requireNonNull(consumer);

        long writerNumber = WRITERS_COUNT.getAndIncrement();
        LOG.debug("Creating writer #{}", writerNumber);
        return (T value) -> CompletableFuture
                .runAsync(() -> {
                    LOG.debug("Writer #{}: Starting write operation", writerNumber);
                    consumer.accept(value);
                    LOG.debug("Writer #{}: Write operation completed", writerNumber);
                })
                .thenApply((unused) -> value);
    }

    /**
     * Creates an ETLWriter that uses a specified function for the write operation.
     * <p>
     * This static method allows users to create an instance of ETLWriter using a Function.
     * The provided function processes the data before writing it asynchronously.
     *
     * @param <T>      The type of data to write.
     * @param function The function that processes the data before writing.
     * @return A new ETLWriter instance that writes data using the provided function.
     */
    static <T> ETLWriter<T> createWriter(Function<T, T> function) {
        Objects.requireNonNull(function);

        long writerNumber = WRITERS_COUNT.getAndIncrement();
        LOG.debug("Creating writer #{}", writerNumber);
        return (T value) -> CompletableFuture
                .supplyAsync(() -> {
                    LOG.debug("Writer #{}: Starting write operation", writerNumber);
                    T t = function.apply(value);
                    LOG.debug("Writer #{}: Write operation completed", writerNumber);
                    return t;
                });
    }
}

