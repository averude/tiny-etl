package io.github.averude.etl;

import io.github.averude.etl.reader.ETLChainedReader;
import io.github.averude.etl.reader.ETLReader;
import io.github.averude.etl.writer.ETLWriter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * ETLBuilder class serves as the entry point for building an ETL (Extract, Transform, Load) pipeline.
 */
@Slf4j
public class ETLBuilder {

    /**
     * Initiates the ETL process by specifying a data reader.
     *
     * @param <T>    The type of the data being read.
     * @param reader The ETLReader instance responsible for reading the data.
     * @return An ETLReaderBuilder that allows further transformation of the data.
     */
    public <T> ETLReaderBuilder<T> read(ETLReader<T> reader) {
        log.trace("Created builder with reader");
        return new ETLReaderBuilder<>(reader);
    }

    /**
     * Builder class for chaining transformations on the data read by an ETLReader.
     *
     * @param <T> The type of data being read and transformed.
     */
    @Slf4j
    public static final class ETLReaderBuilder<T> {

        private final ETLReader<T> reader;

        /**
         * Constructs an ETLReaderBuilder with the specified reader.
         *
         * @param reader The ETLReader instance for reading the data.
         */
        private ETLReaderBuilder(ETLReader<T> reader) {
            this.reader = reader;
        }

        /**
         * Chains another reader to the ETL pipeline.
         * The result from the current reader will be used as input for the chained reader.
         *
         * @param <R>           The type of the data produced by the chained reader.
         * @param chainedReader The ETLChainedReader to be chained after the current reader.
         * @return A new ETLReaderBuilder with the chained reader's result type.
         */
        public <R> ETLReaderBuilder<R> chain(ETLChainedReader<T, R> chainedReader) {
            log.trace("Adding chained read operation");
            return new ETLReaderBuilder<>(() -> reader.read().thenCompose(t -> {
                log.debug("Calling next reader");
                return chainedReader.read(t);
            }));
        }

        /**
         * Chains another reader to the ETL pipeline, allowing for a two-step transformation.
         * The result from the current reader is passed to the chained reader, and both the original
         * data and the result from the chained reader are used to produce a final transformed result.
         *
         * @param <R1>           The type of data produced by the chained reader.
         * @param <R2>           The type of the final transformed data.
         * @param chainedReader  The ETLChainedReader to be executed after the current reader,
         *                       which processes the output of the current reader.
         * @param accumulator     A BiFunction that takes the original data and the result
         *                       from the chained reader, producing a combined result of type R2.
         * @return A new ETLReaderBuilder with the final transformed data type R2, allowing
         *         further transformations or execution configuration to be applied.
         */
        public <R1, R2> ETLReaderBuilder<R2> chain(ETLChainedReader<T, R1> chainedReader,
                                                   BiFunction<T, R1, R2> accumulator) {
            log.trace("Adding chained read operation with accumulation");
            return new ETLReaderBuilder<>(() -> reader.read()
                    .thenCompose(t -> {
                        log.debug("Calling next chained reader");
                        return chainedReader.read(t)
                                .thenApply(r1 -> {
                                    log.debug("Applying accumulator function to readers result");
                                    return accumulator.apply(t, r1);
                                });
                    }));
        }

        /**
         * Applies a transformation to the data read from the reader.
         *
         * @param <R>    The type of the transformed data.
         * @param mapper The function to apply to the data for transformation.
         * @return A new ETLReaderBuilder with the transformed data type.
         */
        public <R> ETLReaderBuilder<R> map(Function<T, R> mapper) {
            log.trace("Adding transformation operation");
            return new ETLReaderBuilder<>(() -> reader.read().thenApply(mapper));
        }

        /**
         * Specifies the writer that will write the transformed data.
         *
         * @param writer The ETLWriter instance responsible for writing the data.
         * @return An ETLExecutorBuilder to further configure execution behavior.
         */
        public ETLExecutorBuilder<T> write(ETLWriter<T> writer) {
            log.trace("Adding write operation");
            return new ETLExecutorBuilder<>(reader.read().thenCompose(writer::write));
        }
    }

    /**
     * Builder class for configuring the execution phase of the ETL process.
     *
     * @param <T> The type of data being processed in the pipeline.
     */
    public static final class ETLExecutorBuilder<T> {

        private final CompletableFuture<T> future;

        /**
         * Constructs an ETLExecutorBuilder with the specified future result of the ETL process.
         *
         * @param future A CompletableFuture representing the ETL pipeline's result.
         */
        private ETLExecutorBuilder(CompletableFuture<T> future) {
            this.future = future;
        }

        /**
         * Applies a transformation to the result of the ETL pipeline.
         *
         * @param <R>    The type of the transformed result.
         * @param mapper The function to apply to the result.
         * @return A new ETLExecutorBuilder with the transformed result type.
         */
        public <R> ETLExecutorBuilder<R> map(Function<T, R> mapper) {
            log.trace("Adding transformation operation");
            return new ETLExecutorBuilder<>(future.thenApply(mapper));
        }

        /**
         * Adds a post-write action to be executed after writing the data.
         *
         * @param postWrite A Consumer to perform an action on the written data.
         * @return A new ETLExecutorBuilder with the post-write action applied.
         */
        public ETLExecutorBuilder<T> postWrite(Consumer<T> postWrite) {
            log.trace("Adding post write operation");
            return new ETLExecutorBuilder<>(future.thenApply((T t) -> {
                log.debug("Calling post write operation");
                postWrite.accept(t);
                return t;
            }));
        }

        /**
         * Executes the ETL pipeline, blocking until completion.
         */
        public void execute() {
            log.info("Executing ETL pipeline");
            future.join();
            log.info("ETL pipeline successfully executed");
        }
    }
}
