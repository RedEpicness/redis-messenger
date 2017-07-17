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
            if (!m.isReply) messageReceived(m)
        } catch (e: Exception) {
            RuntimeException("Exception was uncaught in messageReceived!", e).printStackTrace()
        }

    }

    override fun subscribed(p0: String?, p1: Long) {}

    override fun message(p0: String?, p1: String?, p2: String?) {}

    override fun unsubscribed(p0: String?, p1: Long) {}

    override fun psubscribed(p0: String?, p1: Long) {}

    override fun punsubscribed(p0: String?, p1: Long) {}

    abstract fun messageReceived(msg: RedisMessage)

}
