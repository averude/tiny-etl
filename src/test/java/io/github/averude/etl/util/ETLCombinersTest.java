package io.github.averude.etl.util;

import io.github.averude.etl.reader.ETLReader;
import io.github.averude.etl.writer.ETLWriter;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ETLCombinersTest {
    private static final String DATA = "data";

    @Test
    public void testCombineReaders() {
        ETLReader<Integer> reader1 = Mockito.mock(ETLReader.class);
        ETLReader<String> reader2 = Mockito.mock(ETLReader.class);
        BiFunction<Integer, String, String> combineFunction = (i, s) -> i + s;

        when(reader1.read()).thenReturn(CompletableFuture.completedFuture(1));
        when(reader2.read()).thenReturn(CompletableFuture.completedFuture("A"));

        ETLReader<String> combinedReader = ETLCombiners.combine(reader1, reader2, combineFunction);
        CompletableFuture<String> result = combinedReader.read();

        assertEquals("1A", result.join());
    }

    @Test
    public void testCombineReadersSequential() {
        ETLReader<Integer> reader1 = Mockito.mock(ETLReader.class);
        ETLReader<String> reader2 = Mockito.mock(ETLReader.class);
        BiFunction<Integer, String, String> combineFunction = (i, s) -> i + s;

        when(reader1.read()).thenReturn(CompletableFuture.completedFuture(2));
        when(reader2.read()).thenReturn(CompletableFuture.completedFuture("B"));

        ETLReader<String> combinedReader = ETLCombiners.combine(reader1, reader2, combineFunction);
        CompletableFuture<String> result = combinedReader.read();

        assertEquals("2B", result.join());
    }

    @Test
    public void testCombineParallelReaders() {
        ETLReader<Integer> reader1 = Mockito.mock(ETLReader.class);
        ETLReader<String> reader2 = Mockito.mock(ETLReader.class);
        BiFunction<Integer, String, String> combineFunction = (i, s) -> i + s;

        when(reader1.read()).thenReturn(CompletableFuture.completedFuture(3));
        when(reader2.read()).thenReturn(CompletableFuture.completedFuture("C"));

        ETLReader<String> combinedReader = ETLCombiners.combineParallel(reader1, reader2, combineFunction);
        CompletableFuture<String> result = combinedReader.read();

        assertEquals("3C", result.join());
    }

    @Test
    public void testCombineWriters() {
        ETLWriter<String> writer1 = Mockito.mock(ETLWriter.class);
        ETLWriter<String> writer2 = Mockito.mock(ETLWriter.class);

        when(writer1.write(DATA)).thenReturn(CompletableFuture.completedFuture(null));
        when(writer2.write(DATA)).thenReturn(CompletableFuture.completedFuture(null));

        ETLWriter<String> combinedWriter = ETLCombiners.combine(writer1, writer2);
        CompletableFuture<String> result = combinedWriter.write(DATA);

        assertDoesNotThrow(result::join);
        verify(writer1).write(DATA);
        verify(writer2).write(DATA);
    }

    @Test
    public void testCombineParallelWriters() {
        ETLWriter<String> writer1 = Mockito.mock(ETLWriter.class);
        ETLWriter<String> writer2 = Mockito.mock(ETLWriter.class);

        when(writer1.write(DATA)).thenReturn(CompletableFuture.completedFuture(null));
        when(writer2.write(DATA)).thenReturn(CompletableFuture.completedFuture(null));

        ETLWriter<String> combinedWriter = ETLCombiners.combineParallel(writer1, writer2);
        CompletableFuture<String> result = combinedWriter.write(DATA);

        assertDoesNotThrow(result::join);
        verify(writer1).write(DATA);
        verify(writer2).write(DATA);
    }

    @Test
    public void testCombineWritersThrowsExceptionForLessThanTwoWriters() {
        ETLWriter<String> writer1 = Mockito.mock(ETLWriter.class);

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            ETLCombiners.combine(writer1);
        });

        assertEquals("At least two writers are required", exception.getMessage());
    }

    @Test
    public void testCombineParallelWritersThrowsExceptionForLessThanTwoWriters() {
        ETLWriter<Integer> reader1 = Mockito.mock(ETLWriter.class);

        Exception exception = assertThrows(IllegalArgumentException.class, () -> ETLCombiners.combineParallel(reader1));

        assertEquals("At least two writers are required", exception.getMessage());
    }
}
