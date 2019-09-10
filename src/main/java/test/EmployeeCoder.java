package test;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class EmployeeCoder extends AtomicCoder<Employee> {

    private static final EmployeeCoder INSTANCE = new EmployeeCoder();

    private Coder<Integer> intCoder = VarIntCoder.of();
    private Coder<String> stringCoder = StringUtf8Coder.of();

    public static EmployeeCoder of() {
        return INSTANCE;
    }

    private EmployeeCoder() {}

    @Override
    public void encode(Employee value, OutputStream outStream) throws IOException {
        intCoder.encode(value.getId(), outStream);
        intCoder.encode(value.getRelation(), outStream);
        stringCoder.encode(value.getName(), outStream);
        intCoder.encode(value.getAge(), outStream);
    }

    @Override
    public Employee decode(InputStream inStream) throws IOException {
        int id = intCoder.decode(inStream);
        int relation = intCoder.decode(inStream);
        String name = stringCoder.decode(inStream);
        int age = intCoder.decode(inStream);
        return new Employee(id, relation, name,age);
    }
}
