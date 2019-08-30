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
        val g = "This is a test string"
        val h = '?'
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

        val a1: Double = data.getData("a")
        val b1: Float = data.getData("b")
        val c1: Long = data.getData("c")
        val d1: Int = data.getData("d")
        val e1: Short = data.getData("e")
        val f1: Byte = data.getData("f")
        val g1: String = data.getData("g")
        val h1: Char = data.getData("h")
        val i1: Boolean = data.getData("i")
        val j1: Duration = data.getData("j")

        val id1 = data.id

        assert(id == id1)

        assert(a == a1)
        assert(b == b1)
        assert(c == c1)
        assert(d == d1)
        assert(e == e1)
        assert(f == f1)
        assert(g == g1)
        assert(h == h1)
        assert(i == i1)
        assert(j == j1)

        assert(j === j1)
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
        val g = "This is a test string"
        val h = '?'
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

        val data1 = deserializeData(data.serialize())

        val a1: Double = data1.getData("a")
        val b1: Float = data1.getData("b")
        val c1: Long = data1.getData("c")
        val d1: Int = data1.getData("d")
        val e1: Short = data1.getData("e")
        val f1: Byte = data1.getData("f")
        val g1: String = data1.getData("g")
        val h1: Char = data1.getData("h")
        val i1: Boolean = data1.getData("i")
        val j1: Duration = data1.getData("j")

        val id1 = data1.id

        assert(id == id1)

        assert(a == a1)
        assert(b == b1)
        assert(c == c1)
        assert(d == d1)
        assert(e == e1)
        assert(f == f1)
        assert(g == g1)
        assert(h == h1)
        assert(i == i1)
        assert(j == j1)
    }

}

