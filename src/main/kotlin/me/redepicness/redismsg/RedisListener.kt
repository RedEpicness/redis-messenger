package me.redepicness.redismsg

import io.lettuce.core.pubsub.RedisPubSubListener

/**
 * @author Red_Epicness
 */

abstract class RedisListener : RedisPubSubListener<String, String> {

    override fun message(channel: String?, message: String?) {
        try {
            if (channel == null || message == null)
                TODO("Implement RedisMessage")
            val m = deserializeMsg(channel, message)
            if (!m.isReply) messageRecieved(m)
        } catch (e: Exception) {
            RuntimeException("Exception was uncaught in messageReceived!", e).printStackTrace()
        }

    }

    override fun subscribed(p0: String?, p1: Long) {}

    override fun message(p0: String?, p1: String?, p2: String?) {}

    override fun unsubscribed(p0: String?, p1: Long) {}

    override fun psubscribed(p0: String?, p1: Long) {}

    override fun punsubscribed(p0: String?, p1: Long) {}

    abstract fun messageRecieved(msg: RedisMessage)

}
