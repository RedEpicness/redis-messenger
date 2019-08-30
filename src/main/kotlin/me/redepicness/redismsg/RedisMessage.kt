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

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import java.time.Instant
import java.util.*

/**
 * @author Red_Epicness
 */

fun deserializeMsg(channel: String, message: String): RedisMessage {
    val `object` = JsonParser().parse(message).asJsonObject
    val msg = RedisMessage(
            `object`.get("sender").asString,
            channel,
            deserializeData(JsonParser().parse(`object`.get("data").asString).asJsonObject)
    )
    msg.isReply = `object`.get("isReply").asBoolean
    if (msg.isReply) msg.replyUUID = UUID.fromString(`object`.get("replyUUID").asString)
    msg.timeSent = Instant.parse(`object`.get("timeSent").asString)
    msg.timeReceived = Instant.now()
    msg.uuid = UUID.fromString(`object`.get("uuid").asString)
    return msg
}

class RedisMessage(val sender: String, val channel: String, val data: RedisData) {

    var timeSent: Instant = Instant.now()
        internal set
    var uuid: UUID = UUID.randomUUID()
        internal set
    var timeReceived: Instant = Instant.MIN
        internal set
    var isReply = false
        internal set
    var replyUUID: UUID = UUID.randomUUID()
        internal set

    fun reply(data: RedisData) {
        RedisMessenger.log("Sync reply called with: ")
        RedisMessenger.log(data.toString())
        RedisMessenger.localInstance!!.sendReplyMessage(sender, uuid, data)
    }

    fun replyAsync(data: RedisData) {
        RedisMessenger.log("Async reply called with: ")
        RedisMessenger.log(data.toString())
        RedisMessenger.localInstance!!.sendReplyMessageAsync(sender, uuid, data)
    }

    fun serialize(): String {
        val message = JsonObject()
        message.addProperty("uuid", uuid.toString())
        message.addProperty("timeSent", timeSent.toString())
        message.addProperty("sender", sender)
        message.addProperty("data", data.serialize().toString())
        message.addProperty("isReply", isReply)
        if (isReply) message.addProperty("replyUUID", replyUUID.toString())
        return message.toString()
    }

    internal fun makeReply(uuid: UUID) {
        replyUUID = uuid
        isReply = true
    }

    override fun toString() = "RedisMessage[sender = $sender, channel = $channel, data = $data, timeSent = $timeSent, uuid = $uuid, timeReceived = $timeReceived, isReply = $isReply, replyUUID = $replyUUID]"

    override fun equals(other: Any?): Boolean {
        if (other == null || other !is RedisMessage) return false
        if (other === this) return true
        val msg: RedisMessage = other
        return msg.sender == this.sender &&
                msg.channel == this.channel &&
                msg.data == this.data &&
                msg.timeSent == this.timeSent &&
                msg.uuid == this.uuid &&
                msg.timeReceived == this.timeReceived &&
                msg.isReply == this.isReply &&
                msg.replyUUID == this.replyUUID
    }

    override fun hashCode(): Int {
        return arrayOf(sender, channel, data, timeSent, uuid, timeReceived, isReply, replyUUID).contentHashCode()
    }

}
