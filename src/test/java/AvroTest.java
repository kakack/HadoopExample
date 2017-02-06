import com.Avro.User;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.UTF8;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

/**
 * Created by kakack on 2017/1/20.
 */
public class AvroTest {

    @Test
    public void Serialization() throws IOException {
        User user1 = new User();
        user1.setName("Huyifan");
        user1.setFavoriteColor("Yello");
        user1.setFavoriteNumber(1024);

        User user2 = new User("Pengdameng", 12, "Red");

        User user3 = User.newBuilder()
                .setName("Xidongdong")
                .setFavoriteColor("Blue")
                .setFavoriteNumber(32).build();

        String path = "/Users/apple/Personal/GitKaka/HadoopExample/data/avro/user(1).avsc";
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), new File(path));

        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();

    }

    @Test
    public void Deserialization() throws IOException {
        DatumReader<User> reader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader =
                new DataFileReader<User>(new File("/Users/apple/Personal/GitKaka/HadoopExample/data/avro/user(1).avsc"), reader);
        User user = null;
        while (dataFileReader.hasNext()){
            user = dataFileReader.next();
            System.out.println(user);
        }
    }

    public static void main(String[] args) throws Exception{
        Result result = JUnitCore.runClasses(AssertTest.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(result.wasSuccessful());
        }
    }

}
