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

package me.redepicness.redismsg.tests

import me.redepicness.redismsg.RedisData
import me.redepicness.redismsg.deserializeData
import org.junit.Test
import java.time.Duration
import java.util.*

/**
 * @author Red_Epicness
 */

class RedisDataTest {

    @Test
    fun testWithoutSerialization() {
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

        val a_: Double = data.getData("a")
        val b_: Float = data.getData("b")
        val c_: Long = data.getData("c")
        val d_: Int = data.getData("d")
        val e_: Short = data.getData("e")
        val f_: Byte = data.getData("f")
        val g_: String = data.getData("g")
        val h_: Char = data.getData("h")
        val i_: Boolean = data.getData("i")
        val j_: Duration = data.getData("j")

        val id_ = data.id

        assert(id == id_)

        assert(a == a_)
        assert(b == b_)
        assert(c == c_)
        assert(d == d_)
        assert(e == e_)
        assert(f == f_)
        assert(g == g_)
        assert(h == h_)
        assert(i == i_)
        assert(j == j_)

        assert(j === j_)
    }

    @Test
    fun testWithSerialization() {
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

        val data_ = deserializeData(data.serialize())

        val a_: Double = data_.getData("a")
        val b_: Float = data_.getData("b")
        val c_: Long = data_.getData("c")
        val d_: Int = data_.getData("d")
        val e_: Short = data_.getData("e")
        val f_: Byte = data_.getData("f")
        val g_: String = data_.getData("g")
        val h_: Char = data_.getData("h")
        val i_: Boolean = data_.getData("i")
        val j_: Duration = data_.getData("j")

        val id_ = data_.id

        assert(id == id_)

        assert(a == a_)
        assert(b == b_)
        assert(c == c_)
        assert(d == d_)
        assert(e == e_)
        assert(f == f_)
        assert(g == g_)
        assert(h == h_)
        assert(i == i_)
        assert(j == j_)
    }

}

