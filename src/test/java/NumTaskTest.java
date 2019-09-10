import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import test.Employee;
import test.EmployeeCoder;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;
import java.util.Random;

public class NumTaskTest {
    private static final Random RND = new Random();
    private static final Coder<Employee> EMPLOYEE_CODER = EmployeeCoder.of();
    private static final int NUMBER_OF_EMPLOYEES = 10_000;

    private static final JavaSparkContext JSC = new JavaSparkContext(getSparkConf());
    private static final Path TARGET_FOLDER = new File("target").toPath();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Pipeline pipeline;

    @Before
    public void setUp() {
        final SparkContextOptions options = PipelineOptionsFactory.create().as(SparkContextOptions.class);
        options.setProvidedSparkContext(JSC);
        options.setUsesProvidedSparkContext(true);
        options.setStorageLevel("MEMORY_AND_DISK");
        options.setRunner(SparkRunner.class);
        pipeline = Pipeline.create(options);
    }

    @After
    public void cleanUp() throws IOException {
        final Path rootPath = folder.getRoot().toPath();
        Files.walk(rootPath)
                .filter(src -> !Objects.equals(rootPath, src))
                .forEach(src -> doCopy(src, rootPath.relativize(src)));
    }

    private void doCopy(Path src, Path relativePath) {
        try {
            final Path destination = TARGET_FOLDER.resolve(relativePath);
            if (!destination.toFile().exists()) {
                Files.copy(src, destination);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private static SparkConf getSparkConf() {
        final SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.master", "local[7]");
        sparkConf.set("spark.app.name", "Num Task test");
        sparkConf.set("spark.eventLog.enabled", "true");
        return sparkConf;
    }

    @Test
    public void test() throws IOException {
        PCollection<KV<Integer, Employee>> joined = createEmployees();

        for (int i = 0; i < 10; i++) {
            joined = PCollectionList.of(joined)
                    .and(createEmployees())
                    .apply("Flatten " + i, Flatten.pCollections())
                    .apply("Reshuffle " + i, Reshuffle.of());
        }

        joined.apply("count", Count.perKey())
                .apply("to text", MapElements.via(new ToString()))
                .apply("output", TextIO.write()
                        .to(folder.newFolder("output").getAbsolutePath() + "/count-")
                        .withSuffix(".csv")
                        .withNumShards(1));
        pipeline.run().waitUntilFinish();

        System.out.println("Done.");
    }

    private PCollection<KV<Integer, Employee>> createEmployees() {
        return pipeline.apply(Create.of(testData()))
                .apply("Add Id", WithKeys.of(Employee::getRelation))
                .setCoder(KvCoder.of(VarIntCoder.of(), EMPLOYEE_CODER));
    }

    private Iterable<Employee> testData() {
        return () -> new Iterator<Employee>() {
            private int count = 0;

            @Override
            public boolean hasNext() {
                return count < NUMBER_OF_EMPLOYEES;
            }

            @Override
            public Employee next() {
                count++;
                return createRandomEmployee(count);
            }
        };
    }

    private Employee createRandomEmployee(int id) {
        final int relation = RND.nextInt(NUMBER_OF_EMPLOYEES);
        final int age = RND.nextInt(120);
        final String name = RandomStringUtils.randomAlphabetic(50);
        return new Employee(id, relation, name, age);
    }

    private static class FlattenEmployee extends DoFn<KV<Integer, Iterable<Employee>>, KV<Integer, Employee>> {
        @ProcessElement
        public void process(@Element KV<Integer, Iterable<Employee>> input, OutputReceiver<KV<Integer, Employee>> output) {
            input
                    .getValue()
                    .forEach(e -> output.output(KV.of(input.getKey(), e)));
        }
    }

    private static class ToString extends SimpleFunction<KV<Integer, Long>, String> {
        @Override
        public String apply(KV<Integer, Long> input) {
            return String.format("%d, %d", input.getKey(), input.getValue());
        }
    }

    private static class FlattenGrouped extends DoFn<KV<Integer, Iterable<Employee>>, KV<Integer, Employee>> {
        @ProcessElement
        public void process(@Element KV<Integer, Iterable<Employee>> input, OutputReceiver<KV<Integer, Employee>> output) {
            input.getValue().forEach(v -> output.output(KV.of(input.getKey(), v)));
        }
    }
}
