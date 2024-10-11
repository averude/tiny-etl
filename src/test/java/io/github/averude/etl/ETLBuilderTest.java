package io.github.averude.etl;

import lombok.Data;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.averude.etl.reader.ETLChainedReader.createReader;
import static io.github.averude.etl.reader.ETLReader.createReader;
import static io.github.averude.etl.reader.ETLReader.createSequentialReader;
import static io.github.averude.etl.util.ETLCombiners.combine;
import static io.github.averude.etl.util.ETLCombiners.combineParallel;
import static io.github.averude.etl.writer.ETLWriter.createWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ETLBuilderTest {
    private static final String HELLO_WORLD = "Hello World";
    private static final String HELLO = "Hello";
    public static final String WORLD = " World";

    @Test
    void executeSimpleETL() {
        new ETLBuilder()
                .read(createReader(() -> HELLO_WORLD))
                .write(createWriter(result -> {
                    assertEquals(HELLO_WORLD, result);
                }))
                .execute();
    }

    @Test
    void executeSimpleETLWithMap() {
        new ETLBuilder()
                .read(createReader(() -> HELLO))
                .map(v -> v + WORLD)
                .write(createWriter(result -> {
                    assertEquals(HELLO_WORLD, result);
                }))
                .execute();
    }

    @Test
    void executeSimpleETLWithMapAndPostWrite() {
        var expectedPostWriteResult = 10;
        var resultHolder = new ResultHolder<String>();
        var portWriteHolder = new ResultHolder<Integer>();

        new ETLBuilder()
                .read(createReader(() -> HELLO))
                .map(v -> v + WORLD)
                .write(createWriter(resultHolder::setResult))
                .map(v -> expectedPostWriteResult)
                .postWrite(portWriteHolder::setResult)
                .execute();

        assertEquals(HELLO_WORLD, resultHolder.getResult());
        assertEquals(expectedPostWriteResult, portWriteHolder.getResult());
    }

    @Test
    void executeEtlWithRuntimeException_throwsUncheckedException() {
        var executorBuilder = new ETLBuilder()
                .read(createReader(() -> {
                    throw new ETLUncheckedException();
                }))
                .write(createWriter(unused -> {
                }));

        assertThrows(CompletionException.class, executorBuilder::execute);
    }

    @Test
    void executeEtlWithCombinedSequentialReaders() {
        var resultHolder = new ResultHolder<String>();

        new ETLBuilder()
                .read(
                        combine(
                                createReader(() -> HELLO),
                                createReader(() -> WORLD),
                                (v1, v2) -> v1 + v2
                        )
                )
                .write(createWriter(resultHolder::setResult))
                .execute();

        assertEquals(HELLO_WORLD, resultHolder.getResult());
    }

    @Test
    void executeEtlWithCombinedSequentialWriters() {
        var expectedResult = HELLO_WORLD;
        var firstResultHolder = new ResultHolder<String>();
        var secondResultHolder = new ResultHolder<String>();

        new ETLBuilder()
                .read(createReader(() -> expectedResult))
                .write(
                        combine(
                                createWriter(firstResultHolder::setResult),
                                createWriter(secondResultHolder::setResult)
                        )
                )
                .execute();

        assertEquals(expectedResult, firstResultHolder.getResult());
        assertEquals(expectedResult, secondResultHolder.getResult());
    }

    @Test
    void executeEtlWithCombinedParallelReaders() {
        for (int i = 0; i < 100; i++) {
            var resultHolder = new ResultHolder<String>();
            var firstReaderThreadId = new AtomicLong();
            var secondReaderThreadId = new AtomicLong();

            new ETLBuilder()
                    .read(
                            combineParallel(
                                    createReader(() -> {
                                        sleep(30);
                                        firstReaderThreadId.set(Thread.currentThread().getId());
                                        return HELLO;
                                    }),
                                    createReader(() -> {
                                        sleep(15);
                                        secondReaderThreadId.set(Thread.currentThread().getId());
                                        return WORLD;
                                    }),
                                    (v1, v2) -> v1 + v2
                            )
                    )
                    .write(createWriter(resultHolder::setResult))
                    .execute();

            assertEquals(HELLO_WORLD, resultHolder.getResult());
            assertNotEquals(firstReaderThreadId.get(), secondReaderThreadId.get());
        }
    }

    @Test
    void executeEtlWithCombinedParallelWriters() {
        for (int i = 0; i < 100; i++) {
            var firstResultHolder = new ResultHolder<String>();
            var secondResultHolder = new ResultHolder<String>();
            var firstReaderThreadId = new AtomicLong();
            var secondReaderThreadId = new AtomicLong();

            new ETLBuilder()
                    .read(createReader(() -> HELLO_WORLD))
                    .write(
                            combineParallel(
                                    createWriter(result -> {
                                        sleep(4);
                                        firstReaderThreadId.set(Thread.currentThread().getId());
                                        firstResultHolder.setResult(result);
                                    }),
                                    createWriter(result1 -> {
                                        sleep(3);
                                        secondReaderThreadId.set(Thread.currentThread().getId());
                                        secondResultHolder.setResult(result1);
                                    })
                            )
                    )
                    .execute();

            assertEquals(HELLO_WORLD, firstResultHolder.getResult());
            assertEquals(HELLO_WORLD, secondResultHolder.getResult());
            assertNotEquals(firstReaderThreadId.get(), secondReaderThreadId.get());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void complexScenarioETL() {
        var expectedResult = "1234 after mapping";
        Repository<String> repoOne = mock(Repository.class);
        Repository<String> repoTwo = mock(Repository.class);
        Repository<String> repoThree = mock(Repository.class);
        Repository<String> repoFour = mock(Repository.class);

        when(repoOne.read()).thenReturn("1");
        when(repoTwo.read()).thenReturn("2");
        when(repoThree.read()).thenReturn("3");
        when(repoFour.read()).thenReturn("4");

        new ETLBuilder()
                .read(
                        combineParallel(
                                combineParallel(
                                        createReader(repoOne::read),
                                        createReader(repoTwo::read),
                                        (v1, v2) -> v1 + v2
                                ),
                                combineParallel(
                                        createReader(repoThree::read),
                                        createReader(repoFour::read),
                                        (v1, v2) -> v1 + v2
                                ),
                                (v1, v2) -> v1 + v2
                        )
                )
                .map(v1 -> v1 + " after mapping")
                .write(
                        combineParallel(
                                combine(
                                        createWriter(repoOne::write),
                                        createWriter(repoTwo::write)
                                ),
                                combine(
                                        createWriter(repoThree::write),
                                        createWriter(repoFour::write)
                                )
                        )
                )
                .map(v -> v)
                .postWrite(v -> assertEquals(expectedResult, v))
                .execute();

        verify(repoOne).write(expectedResult);
        verify(repoTwo).write(expectedResult);
        verify(repoThree).write(expectedResult);
        verify(repoFour).write(expectedResult);
    }

    // Read first, use first read for second read return combination of first and second read
    @Test
    void complexScenario() {
        new ETLBuilder()
                .read(
                        combine(
                                createSequentialReader(
                                        () -> HELLO,
                                        (result) -> result + WORLD
                                ),
                                createSequentialReader(
                                        () -> HELLO, String::length, (k1, v11) -> {
                                            var stringIntegerHashMap = new HashMap<>();
                                            stringIntegerHashMap.put(k1, v11);
                                            return stringIntegerHashMap;
                                        }),
                                (v1, v2) -> {
                                    v2.put(v1, 30);
                                    return v2;
                                }
                        )
                )
                .write(createWriter((v) -> {
                    assertFalse(v.isEmpty());
                    assertEquals(2, v.size());
                    assertTrue(v.containsKey("Hello"));
                    assertTrue(v.containsKey(HELLO_WORLD));
                }))
                .execute();
    }

    @Test
    void chainedRead() {
        new ETLBuilder()
                .read(createReader(() -> HELLO))
                .chain(createReader(val -> val + WORLD))
                .map(val -> val + "!")
                .write(createWriter(v -> {
                    assertEquals(HELLO_WORLD + "!", v);
                }));
    }

    @Test
    void chainedReadWithCombine() {
        new ETLBuilder()
                .read(createReader(() -> HELLO))
                .chain(
                        combine(
                                createReader(val -> val + WORLD),
                                createReader(val -> val + " User"),
                                (v1, v2) -> v1 + ". " + v2
                        )
                )
                .map(val -> val + "!")
                .write(createWriter(v -> {
                    assertEquals("Hello World. Hello User!", v);
                }));
    }

    @Test
    void chainedReadWithParallelCombine() {
        new ETLBuilder()
                .read(createReader(() -> HELLO))
                .chain(
                        combineParallel(
                                createReader(val -> {
                                    sleep(10);
                                    return val + WORLD;
                                }),
                                createReader(val -> {
                                    sleep(100);
                                    return val + " User";
                                }),
                                (v1, v2) -> v1 + ". " + v2
                        )
                )
                .map(val -> val + "!")
                .write(createWriter(v -> {
                    assertEquals("Hello World. Hello User!", v);
                }));
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    private static final class ETLUncheckedException extends RuntimeException {
    }

    @Data
    private static final class ResultHolder<T> {
        private T result;
    }

    interface Repository<T> {
        T read();

        T write(T t);
    }
}