import org.apache.hadoop.io.Text;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;



import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 * Created by kakack on 2017/1/19.
 */
public class AssertTest {

//   public static void main(String[] args) throws Exception {
//        Text t = new Text("hello Hadoop");
//        System.out.println(t.charAt(4));
//    }

    @Test
    public void AssertTest() throws Exception{
        Text t = new Text("Hello Hadoop");
        assertThat(t.getLength(), is(12));
        assertThat(t.charAt(0), is(72));
        System.out.println("done!");
    }

    public static void main(String[] args) throws Exception{
        Result result = JUnitCore.runClasses(AssertTest.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(result.wasSuccessful());
        }
    }
}
