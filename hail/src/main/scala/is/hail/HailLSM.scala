package is.hail

import java.io.{ DataInput, DataOutput, File }
import com.indeed.lsmtree.core._
import com.indeed.util.serialization._
import com.indeed.util.serialization.array._

class HailLSM {
  val lsm = new StoreBuilder[Long, Long](new File("/tmp/haillsm"),
    new Serializer[Long]() {
      def write(l: Long, out: DataOutput): Unit = {
        out.writeLong(l)
      }
      def read(in: DataInput): Long = {
        in.readLong()
      }
    }, new ByteArraySerializer()).build()
}
