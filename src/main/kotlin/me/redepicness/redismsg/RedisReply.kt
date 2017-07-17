package me.redepicness.redismsg

import java.time.Duration
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Consumer

/**
 * @author Red_Epicness
 */
class RedisReply(private val message: RedisMessage, private val timeout: Duration, private val async: Boolean) {

    fun callback(callback: Consumer<Optional<RedisMessage>>) {
        RedisMessenger.log("Get value with callback!")
        RedisMessenger.localInstance!!.scheduleReply(message.uuid, timeout, Consumer { m ->
            RedisMessenger.log("Success!")
            if (m == null)
                callback.accept(Optional.empty<RedisMessage>())
            else
                callback.accept(Optional.of(m))
        })
        publishMessage()
    }

    fun get(): Optional<RedisMessage> {
        RedisMessenger.log("Get value with blocking!")
        val lock = ReentrantLock()
        val condition = lock.newCondition()
        val capsule = Capsule<RedisMessage>(null)
        try {
            lock.lock()
            RedisMessenger.localInstance!!.scheduleReply(message.uuid, timeout, Consumer { m ->
                lock.lock()
                try {
                    capsule.set(m!!)
                } finally {
                    condition.signal()
                    lock.unlock()
                }
            })
            publishMessage()
            condition.await()
        } catch (e1: InterruptedException) {
            e1.printStackTrace()
        } finally {
            lock.unlock()
        }
        val message = capsule.get()
        RedisMessenger.log("Value retrieved!")
        if (message == null)
            return Optional.empty<RedisMessage>()
        else
            return Optional.of(message)
    }

    private fun publishMessage() {
        RedisMessenger.log("Publishing reply message!")
        if (async) {
            RedisMessenger.localInstance!!.pubSubCommands.publish(message.channel, message.serialize())
        } else {
            RedisMessenger.localInstance!!.pubSubAsyncCommands.publish(message.channel, message.serialize())
        }
    }

    private inner class Capsule<T> internal constructor(private var obj: T?) {

        internal fun get(): T? {
            return this.obj
        }

        internal fun set(obj: T) {
            this.obj = obj
        }

    }
}
