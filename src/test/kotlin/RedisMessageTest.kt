import me.redepicness.redismsg.RedisData
import me.redepicness.redismsg.RedisMessage
import me.redepicness.redismsg.deserializeMsg
import org.junit.Test
import java.time.Duration
import java.util.*

/**
 * @author Red_Epicness
 */

class RedisMessageTest {

    @Test
    fun serializationTest() {
        val rand = Random()

        val a: Double = rand.nextDouble()
        val b: Float = rand.nextFloat()
        val c: Long = rand.nextLong()
        val d: Int = rand.nextInt()
        val e: Short = rand.nextInt(Short.MAX_VALUE.toInt()).toShort()
        val f: Byte = rand.nextInt(Byte.MAX_VALUE.toInt()).toByte()
        val g: String = "This is a test string"
        val h: Char = '?'
        val i: Boolean = rand.nextBoolean()
        val j: Duration = Duration.ofSeconds(rand.nextLong())

        val id = "Testing ID"

        val data = RedisData(id)

        data.addDouble("a", a)
        data.addFloat("b", b)
        data.addLong("c", c)
        data.addInt("d", d)
        data.addShort("e", e)
        data.addByte("f", f)
        data.addString("g", g)
        data.addChar("h", h)
        data.addBoolean("i", i)
        data.addObject("j", j)

        val msg = RedisMessage("sender", "channel", data)

        val msg_ = deserializeMsg(msg.channel, msg.serialize())

        assert(msg.sender == msg_.sender)
        assert(msg.channel == msg_.channel)
        assert(msg.data == msg_.data)
        assert(msg.timeSent == msg_.timeSent)
        assert(msg.isReply == msg_.isReply)
        assert(msg.uuid == msg_.uuid)
    }

}
