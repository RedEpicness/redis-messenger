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

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import java.io.*
import java.util.*

/**
 * @author Red_Epicness
 */

typealias IAE = IllegalArgumentException

internal fun deserializeData(data: JsonObject): RedisData = RedisData(data)

class RedisData(val id: String = "Undefined") {

    constructor(obj: JsonObject) : this(obj.get("id").asString) {
        val data = obj.getAsJsonObject("data")
        for ((key, value) in data.entrySet()) {
            val type = Type.valueOf(value.asJsonObject.get("type").asString)
            val d = value.asJsonObject.get("data")
            objects.put(
                    key,
                    when (type) {
                        Type.DOUBLE -> RedisChunk(type, d.asDouble)
                        Type.FLOAT -> RedisChunk(type, d.asFloat)
                        Type.LONG -> RedisChunk(type, d.asLong)
                        Type.INT -> RedisChunk(type, d.asInt)
                        Type.SHORT -> RedisChunk(type, d.asShort)
                        Type.BYTE -> RedisChunk(type, d.asByte)
                        Type.STRING -> RedisChunk(type, d.asString)
                        Type.CHAR -> RedisChunk(type, d.asCharacter)
                        Type.BOOLEAN -> RedisChunk(type, d.asBoolean)
                        Type.OBJECT -> {
                            val array = d.asJsonArray
                            val bytes = ByteArray(array.size())
                            for (i in bytes.indices) {
                                bytes[i] = array.get(i).asByte
                            }
                            val bis = ByteArrayInputStream(bytes)
                            var input: ObjectInput? = null
                            try {
                                input = ObjectInputStream(bis)
                                val o = input.readObject()
                                RedisChunk(type, o)
                            } catch (e: Exception) {
                                throw RuntimeException("Error while de-serializing data", e)
                            } finally {
                                try {
                                    input?.close()
                                    bis.close()
                                } catch (ignored: IOException) {
                                }
                            }
                        }
                    }
            )
        }
    }

    private val objects: HashMap<String, RedisChunk> = HashMap()

    fun addObject(id: String, data: Serializable) {
        objects.put(id, RedisChunk(Type.OBJECT, data))
    }

    fun addDouble(id: String, data: Double) {
        objects.put(id, RedisChunk(Type.DOUBLE, data))
    }

    fun addFloat(id: String, data: Float) {
        objects.put(id, RedisChunk(Type.FLOAT, data))
    }

    fun addLong(id: String, data: Long) {
        objects.put(id, RedisChunk(Type.LONG, data))
    }

    fun addInt(id: String, data: Int) {
        objects.put(id, RedisChunk(Type.INT, data))
    }

    fun addShort(id: String, data: Short) {
        objects.put(id, RedisChunk(Type.SHORT, data))
    }

    fun addByte(id: String, data: Byte) {
        objects.put(id, RedisChunk(Type.BYTE, data))
    }

    fun addString(id: String, data: String) {
        objects.put(id, RedisChunk(Type.STRING, data))
    }

    fun addChar(id: String, data: Char) {
        objects.put(id, RedisChunk(Type.CHAR, data))
    }

    fun addBoolean(id: String, data: Boolean) {
        objects.put(id, RedisChunk(Type.BOOLEAN, data))
    }

    fun <T> getData(id: String) = objects[id]?.data as? T ?: throw IAE("Field $id does not exist!")

    fun hasData(id: String) = objects.containsKey(id)

    private fun serializeChunk(chunk: RedisChunk): JsonObject {
        val data = JsonObject()
        val type = chunk.type
        data.add("type", JsonPrimitive(type.toString()))
        when (type) {
            Type.DOUBLE,
            Type.FLOAT,
            Type.LONG,
            Type.INT,
            Type.SHORT,
            Type.BYTE -> data.add("data", JsonPrimitive(chunk.data as Number))
            Type.STRING -> data.add("data", JsonPrimitive(chunk.data as String))
            Type.CHAR -> data.add("data", JsonPrimitive(chunk.data as Char))
            Type.BOOLEAN -> data.add("data", JsonPrimitive(chunk.data as Boolean))
            Type.OBJECT -> {
                val byteArray = JsonArray()
                val bos = ByteArrayOutputStream()
                var out: ObjectOutput? = null
                try {
                    out = ObjectOutputStream(bos)
                    out.writeObject(chunk.data)
                    out.flush()
                    bos.toByteArray().forEach { byteArray.add(it) }
                } catch (e: IOException) {
                    throw RuntimeException("Error while serializing data", e)
                } finally {
                    try {
                        out?.close()
                        bos.close()
                    } catch (ignored: IOException) {
                    }
                }
                data.add("data", byteArray)
            }
        }
        return data
    }

    internal fun serialize(): JsonObject {
        val data = JsonObject()
        objects.forEach { k, v -> data.add(k, serializeChunk(v)) }
        val obj = JsonObject()
        obj.add("id", JsonPrimitive(id))
        obj.add("data", data)
        return obj
    }

    override fun toString(): String {
        return "RedisData" + serialize().toString()
    }

    override fun equals(other: Any?): Boolean {
        if (other == null || other !is RedisData) return false
        if (other === this) return true
        val data: RedisData = other
        return data.objects == this.objects && data.id == this.id
    }

    override fun hashCode(): Int {
        return Arrays.hashCode(arrayOf(id, objects))
    }

}

private data class RedisChunk(val type: Type, val data: Any)

private enum class Type {

    OBJECT,
    DOUBLE,
    FLOAT,
    LONG,
    INT,
    SHORT,
    BYTE,
    STRING,
    CHAR,
    BOOLEAN

}
