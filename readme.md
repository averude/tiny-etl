# Tiny ETL framework

This framework provides an easy way to create ETL (Extract, Transform, Load) pipelines with support for asynchronous operations. It also includes helpers to combine readers and writers, offering flexibility in how you process and manage data.

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
    - [Reading Data](#reading-data)
    - [Chaining Readers](#chaining-readers)
    - [Transforming Data](#transforming-data)
    - [Writing Data](#writing-data)
    - [Executing the Pipeline](#executing-the-pipeline)
- [Combining Readers and Writers](#combining-readers-and-writers)
    - [Combining Readers](#combining-readers)
    - [Combining Writers](#combining-writers)
- [Examples](#examples)
- [Contributing](#contributing)
- [License](#license)

## Installation

To add the Tiny ETL library to your project, use the following dependency declarations:

### Maven

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.github.averude</groupId>
    <artifactId>tiny-etl</artifactId>
    <version>0.0.1</version>
</dependency>
```

### Gradle

For Gradle, include the following implementation in your `build.gradle`:

```groovy
implementation 'io.github.averude:tiny-etl:0.0.1'
```

## Usage

To use this ETL framework, include the relevant classes and interfaces in your project. The framework includes the following:

- `ETLBuilder`: To construct ETL pipelines.
- `ETLReader`: Functional interface for reading data asynchronously.
- `ETLChainedReader`: Functional interface for chaining multiple readers.
- `ETLWriter`: Functional interface for writing data.
- `ETLExecutorBuilder`: For executing the pipeline after all steps are configured.
- `ETLCombiners`: Utilities to combine multiple readers and writers.

### 1. Reading Data

To begin building your ETL pipeline, define an `ETLReader` to extract data:

```java
ETLReader<String> reader = ETLReader.createReader(() -> "Initial Data");
```

### 2. Chaining Readers

You can chain multiple readers using `chain()`. The output from one reader can be passed to another:

```java
ETLChainedReader<String, Integer> lengthReader = ETLChainedReader.createReader(String::length);
builder.read(reader).chain(lengthReader);
```

### 3. Transforming Data

Use the `map()` function to apply transformations to the data:

```java
builder.read(reader).map(data -> data.toUpperCase());
```

### 4. Writing Data

An `ETLWriter` is used to load the transformed data:

```java
ETLWriter<String> writer = ETLWriter.createWriter(System.out::println);
builder.read(reader).write(writer);
```

### 5. Executing the Pipeline

Once the pipeline is set, execute it using `execute()`:

```java
builder.read(reader).map(data -> data.toUpperCase()).write(writer).execute();
```

## Combining Readers and Writers

The `ETLCombiners` utility class provides methods to combine multiple readers and writers either sequentially or in parallel.

### Combining Readers

#### 1. Sequential Combination

Combine two readers in sequence, where the result of both readers is combined by a function:

```java
ETLReader<String> reader1 = ETLReader.createReader(() -> "Hello");
ETLReader<Integer> reader2 = ETLReader.createReader(() -> 42);

ETLReader<String> combinedReader = ETLCombiners.combine(reader1, reader2, (r1, r2) -> r1 + " " + r2);
```

#### 2. Parallel Combination

Combine two readers to run in parallel:

```java
ETLReader<String> combinedParallelReader = ETLCombiners.combineParallel(reader1, reader2, (r1, r2) -> r1 + " " + r2);
```

#### 3. Combining Chained Readers

You can also combine `ETLChainedReader` objects either sequentially or in parallel:

```java
ETLChainedReader<String, Integer> reader1 = ETLChainedReader.createReader(String::length);
ETLChainedReader<String, Integer> reader2 = ETLChainedReader.createReader(s -> s.hashCode());

ETLChainedReader<String, String> combinedChainedReader = ETLCombiners.combine(reader1, reader2, (r1, r2) -> r1 + "-" + r2);
```

### Combining Writers

#### 1. Sequential Combination

Combine two writers sequentially so that the same data is written to both:

```java
ETLWriter<String> writer1 = ETLWriter.createWriter(s -> {
  System.out.println(s);
});
ETLWriter<String> writer2 = ETLWriter.createWriter(s -> {
  System.out.println("Processed: " + s);
});

ETLWriter<String> combinedWriter = ETLCombiners.combine(writer1, writer2);
```

#### 2. Parallel Combination

Write data to both writers in parallel:

```java
ETLWriter<String> combinedParallelWriter = ETLCombiners.combineParallel(writer1, writer2);
```

## Examples

Here is a simple example of ETL pipeline:

```java
public class ETLExample {
    public static void main(String[] args) {
        ETLBuilder builder = new ETLBuilder();

        // Readers
        ETLReader<String> reader1 = ETLReader.createReader(() -> "Data1");
        ETLReader<String> reader2 = ETLReader.createReader(() -> "Data2");

        // Combine readers
        ETLReader<String> combinedReader = ETLCombiners.combine(reader1, reader2, (r1, r2) -> r1 + " & " + r2);

        // Writers
        ETLWriter<String> writer1 = ETLWriter.createWriter(s -> {
            System.out.println(s);
        });
        ETLWriter<String> writer2 = ETLWriter.createWriter(s -> {
            System.out.println("Processed: " + s);
        });

        // Combine writers
        ETLWriter<String> combinedWriter = ETLCombiners.combine(writer1, writer2);

        // Build and execute the ETL pipeline
        builder.read(combinedReader).write(combinedWriter).execute();
    }
}
```

or in more functional style

```java
public class ETLExample {
    public static void main(String[] args) {
        new ETLBuilder()
                .read(
                        // combined readers
                        combine(
                                createReader(() -> "Data1"),
                                createReader(() -> "Data2"),
                                (r1, r2) -> r1 + " & " + r2
                        )
                )
                .write(
                        // combined writers
                        combine(
                                createWriter(s -> {
                                  System.out.println(s);
                                }),
                                createWriter(s -> {
                                    System.out.println("Processed: " + s);
                                })
                        )
                )
                .execute();
    }
}
```

In this example, the ETL pipeline combines two readers and two writers, processes the combined data, and outputs the result.

### DSL-style examples

#### 1. Simple Read and Write
```java
public class ETLExample {
    public static void main(String[] args) {
        new ETLBuilder()
                .read(createReader(() -> "Simple Data"))
                .write(createWriter(s -> {
                    System.out.println(s);
                }))
                .execute();
    }
}
```

#### 2. Simple Read and Write with Transform of Read Data
```java
public class ETLExample {
    public static void main(String[] args) {
        new ETLBuilder()
                .read(createReader(() -> "raw data"))
                .map(data -> data.toUpperCase()) // Transform data to uppercase
                .write(createWriter(s -> {
                    System.out.println(s);
                }))
                .execute();
    }
}
```

#### 3. Simple Read and Write with Read Chain and Transform of Read Data
```java
public class ETLExample {
    public static void main(String[] args) {
        new ETLBuilder()
                .read(createReader(() -> "data part 1"))
                .chain(createReader((firstReadData) -> firstReadData + " + data PART 2"))
                .map(combinedData -> combinedData.toLowerCase()) // Transform combined data to lowercase
                .write(createWriter(s -> {
                    System.out.println(s);
                }))
                .execute();
    }
}
```

#### 4. Simple Read and Write with Call Map of Written Data and Post Write Action
```java
public class ETLExample {
    public static void main(String[] args) {
        new ETLBuilder()
                .read(createReader(() -> "Output Data"))
                .write(createWriter(s -> {
                    System.out.println(s);
                }))
                .map(data::length)
                .postWrite(System.out::println)
                .execute();
    }
}
```

#### 5. Combined Read and Combined Write (Sequential and Parallel)
```java
public class ETLExample {
    public static void main(String[] args) {
        new ETLBuilder()
                .read(
                        // Combined readers
                        combine(
                                createReader(() -> "Reader 1"),
                                createReader(() -> "Reader 2"),
                                (r1, r2) -> r1 + " | " + r2 // Combine results
                        )
                )
                .write(
                        // Combined writers
                        combine(
                                createWriter(s -> {
                                  System.out.println(s);
                                }),
                                createWriter(s -> {
                                  System.out.println("Processed: " + s);
                                })
                        )
                )
                .execute();

        // Parallel example
        new ETLBuilder()
                .read(
                        // Combined readers in parallel
                        combineParallel(
                                createReader(() -> "Parallel Reader 1"),
                                createReader(() -> "Parallel Reader 2"),
                                (r1, r2) -> r1 + " & " + r2 // Combine results
                        )
                )
                .write(
                        // Combined writers in parallel
                        combineParallel(
                                createWriter(s -> {
                                    System.out.println(s);
                                }),
                                createWriter(s -> {
                                    System.out.println("Processed (Parallel): " + s);
                                })
                        )
                )
                .execute();
    }
}
```

## Contributing

Contributions are welcome! Feel free to submit a pull request or open an issue.

## License

This ETL framework is licensed under the MIT License.