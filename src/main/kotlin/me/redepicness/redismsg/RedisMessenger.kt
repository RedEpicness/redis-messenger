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

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.function.BiConsumer
import java.util.function.Consumer

/**
 * @author Red_Epicness
 */

class RedisMessenger(val id: String, uri: RedisURI) {

    internal var pubSubCommands: RedisPubSubCommands<String, String>
    internal var pubSubAsyncCommands: RedisPubSubAsyncCommands<String, String>
    private val pubSubConnection: StatefulRedisPubSubConnection<String, String>
    private val listenerConnection: StatefulRedisPubSubConnection<String, String>
    private val scheduler: ScheduledExecutorService
    private var customScheduler: BiConsumer<Runnable, Duration>? = null
    private val scheduledReplies: HashMap<UUID, Consumer<RedisMessage?>>

    init {
        if (localInstance == null) throw RuntimeException("Cannot have more than one instance of Redis messenger running!")
        RedisMessenger.localInstance = this
        scheduledReplies = HashMap<UUID, Consumer<RedisMessage?>>()
        scheduler = Executors.newSingleThreadScheduledExecutor()
        listenerConnection = RedisClient.create(uri).connectPubSub()
        listenerConnection.sync().clientSetname(id + "-listener")
        subscribe(id)
        pubSubConnection = RedisClient.create(uri).connectPubSub()
        pubSubCommands = pubSubConnection.sync()
        pubSubCommands.clientSetname(id + "-commands")
        pubSubAsyncCommands = pubSubConnection.async()
        addListeners(object : RedisListener() {
            override fun message(channel: String?, message: String?) {
                try {
                    log("Reply Listener: New message on channel: " + channel!!)
                    val m = deserializeMsg(channel, message!!)
                    if (!m.isReply) {
                        log("Not a reply!")
                        return
                    }
                    log(m.toString())
                    val uuid = m.replyUUID
                    if (scheduledReplies.containsKey(uuid)) {
                        log("Reply uuid found, calling callback!")
                        scheduledReplies[uuid]?.accept(m)
                        scheduledReplies.remove(uuid)
                        log("Success!")
                    } else {
                        log("No callback found for this reply!")
                    }
                } catch (e: Exception) {
                    RuntimeException("Exception was uncaught in messageReceived!", e).printStackTrace()
                }

            }

            override fun messageReceived(msg: RedisMessage) {} //NOT NEEDED
        })
    }

    fun disable() {
        listenerConnection.close()
        pubSubConnection.flushCommands()
        pubSubConnection.close()
    }

    fun addListeners(vararg listeners: RedisListener): RedisMessenger {
        if (!listenerConnection.isOpen) throw RuntimeException("The connection is not open!")
        for (listener in listeners) {
            listenerConnection.addListener(listener)
        }
        return this
    }

    fun removeListeners(vararg listeners: RedisListener): RedisMessenger {
        if (!listenerConnection.isOpen) throw RuntimeException("The connection is not open!")
        for (listener in listeners) {
            listenerConnection.removeListener(listener)
        }
        return this
    }

    fun subscribe(channel: String): RedisMessenger {
        if (!listenerConnection.isOpen) throw RuntimeException("The connection is not open!")
        listenerConnection.sync().subscribe(channel)
        return this
    }

    fun unsubscribe(channel: String): RedisMessenger {
        if (!listenerConnection.isOpen) throw RuntimeException("The connection is not open!")
        listenerConnection.sync().unsubscribe(channel)
        return this
    }

    fun subscribeAsync(channel: String): RedisMessenger {
        if (!listenerConnection.isOpen) throw RuntimeException("The connection is not open!")
        listenerConnection.async().subscribe(channel)
        return this
    }

    fun unsubscribeAsync(channel: String): RedisMessenger {
        if (!listenerConnection.isOpen) throw RuntimeException("The connection is not open!")
        listenerConnection.async().unsubscribe(channel)
        return this
    }

    fun sendMessage(channel: String, data: RedisData): RedisMessage {
        val message = RedisMessage(id, channel, data)
        log("Sending new message sync:")
        log(message.toString())
        pubSubCommands.publish(channel, message.serialize())
        return message
    }

    fun sendMessageAsync(channel: String, data: RedisData): RedisMessage {
        val message = RedisMessage(id, channel, data)
        log("Sending new message async:")
        log(message.toString())
        pubSubAsyncCommands.publish(channel, message.serialize())
        return message
    }

    fun sendMessageWithReply(channel: String, data: RedisData, timeout: Duration): RedisReply {
        val message = RedisMessage(id, channel, data)
        log("Sending new message with reply sync:")
        val redisReply = RedisReply(message, timeout, false)
        log("Reply: " + redisReply.toString())
        return redisReply
    }

    fun sendMessageAsyncWithReply(channel: String, data: RedisData, timeout: Duration): RedisReply {
        val message = RedisMessage(id, channel, data)
        log("Sending new message with reply async:")
        val redisReply = RedisReply(message, timeout, true)
        log("Reply: " + redisReply.toString())
        return redisReply
    }

    fun setCustomScheduler(customScheduler: BiConsumer<Runnable, Duration>) {
        this.customScheduler = customScheduler
    }

    internal fun sendReplyMessage(channel: String, uuid: UUID, data: RedisData) {
        val message = RedisMessage(id, channel, data)
        message.makeReply(uuid)
        pubSubCommands.publish(channel, message.serialize())
    }

    internal fun sendReplyMessageAsync(channel: String, uuid: UUID, data: RedisData) {
        val message = RedisMessage(id, channel, data)
        message.makeReply(uuid)
        pubSubAsyncCommands.publish(channel, message.serialize())
    }

    internal fun scheduleReply(uuid: UUID, timeout: Duration, callback: Consumer<RedisMessage?>) {
        log("Reply scheduled for $uuid, timeout: $timeout")
        val task = Runnable {
            log("reply task running")
            if (scheduledReplies.containsKey(uuid)) {
                log("reply ran out!")
                callback.accept(null)
            }
            scheduledReplies.remove(uuid)
        }
        if (customScheduler == null) {
            scheduler.schedule(task, timeout.toMillis(), TimeUnit.MILLISECONDS)
        } else {
            customScheduler!!.accept(task, timeout)
        }
        scheduledReplies.put(uuid, callback)
    }

    companion object {

        var localInstance: RedisMessenger? = null
            private set
        private var doLogging = false

        fun setLogging(doLogging: Boolean) {
            RedisMessenger.doLogging = doLogging
        }

        internal fun log(message: String) {
            if (doLogging)
                println(message)
        }
    }

}
