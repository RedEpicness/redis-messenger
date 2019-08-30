/*
 * Copyright 2017 Red_Epicness
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package me.redepicness.redismsg

import java.time.Duration
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Consumer

/**
 * This class represents a reply to a message.
 *
 * You can request a reply asynchronously using [callback], or synchronously using [get].
 *
 * @since 1.0
 * @author Red_Epicness
 */
class RedisReply(private val message: RedisMessage, private val timeout: Duration, private val async: Boolean) {

    /**
     * This method allows asynchronous retrieval of a reply message using a callback.
     *
     * Once the message is received by the messenger, [callback] is executed with the optional message as the argument.
     * If the retrieval times-out, an [Optional.empty] is returned.
     *
     * @param callback The Consumer to run after the reply is received.
     *
     * @return The optional [RedisMessage].
     *
     * @since 1.0
     */
    fun callback(callback: Consumer<Optional<RedisMessage>>) {
        RedisMessenger.log("Get value with callback!")
        RedisMessenger.localInstance!!.scheduleReply(message.uuid, timeout, Consumer { m ->
            RedisMessenger.log("Success!")
            if (m == null)
                callback.accept(Optional.empty())
            else
                callback.accept(Optional.of(m))
        })
        publishMessage()
    }

    /**
     * This method allows synchronous retrieval of a reply message.
     *
     * This method blocks until the message is received, or the retrieval times-out.
     * If the retrieval times-out, an [Optional.empty] is returned.
     *
     * @return The optional [RedisMessage].
     *
     * @since 1.0
     */
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
        return if (message == null) Optional.empty() else Optional.of(message)
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
