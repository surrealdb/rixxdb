// Copyright © 2016 Abcum Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package data

import (
	"fmt"
	"math"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var m uint64 = math.MaxUint64

var s = []string{
	"/some",                 // 0
	"/test",                 // 1
	"/test/one",             // 2
	"/test/one/sub-one",     // 3
	"/test/one/sub-one/1st", // 4
	"/test/one/sub-one/2nd", // 5
	"/test/one/sub-two",     // 6
	"/test/one/sub-two/1st", // 7
	"/test/one/sub-two/2nd", // 8
	"/test/one/sub-zen",     // 9
	"/test/one/sub-zen/1st", // 10 ----------
	"/test/one/sub-zen/2nd", // 11
	"/test/two",             // 12
	"/test/two/sub-one",     // 13
	"/test/two/sub-one/1st", // 14
	"/test/two/sub-one/2nd", // 15
	"/test/two/sub-two",     // 16
	"/test/two/sub-two/1st", // 17
	"/test/two/sub-two/2nd", // 18
	"/test/two/sub-zen",     // 19
	"/test/two/sub-zen/1st", // 20
	"/test/two/sub-zen/2nd", // 21
	"/test/zen",             // 22
	"/test/zen/sub-one",     // 23
	"/test/zen/sub-one/1st", // 24
	"/test/zen/sub-one/2nd", // 25
	"/test/zen/sub-two",     // 26
	"/test/zen/sub-two/1st", // 27
	"/test/zen/sub-two/2nd", // 28
	"/test/zen/sub-zen",     // 29
	"/test/zen/sub-zen/1st", // 30
	"/test/zen/sub-zen/2nd", // 31
	"/zoo",                  // 32
	"/zoo/some",             // 33
	"/zoo/some/path",        // 34
}

var p = [][]int{
	{0, 0},                   // 0
	{0, 1},                   // 1
	{0, 1, 0, 0},             // 2
	{0, 1, 0, 0, 0, 0},       // 3
	{0, 1, 0, 0, 0, 0, 0, 0}, // 4
	{0, 1, 0, 0, 0, 0, 0, 1}, // 5
	{0, 1, 0, 0, 0, 1},       // 6
	{0, 1, 0, 0, 0, 1, 0, 0}, // 7
	{0, 1, 0, 0, 0, 1, 0, 1}, // 8
	{0, 1, 0, 0, 0, 2},       // 9
	{0, 1, 0, 0, 0, 2, 0, 0}, // 10 ----------
	{0, 1, 0, 0, 0, 2, 0, 1}, // 11
	{0, 1, 0, 1},             // 12
	{0, 1, 0, 1, 0, 0},       // 13
	{0, 1, 0, 1, 0, 0, 0, 0}, // 14
	{0, 1, 0, 1, 0, 0, 0, 1}, // 15
	{0, 1, 0, 1, 0, 1},       // 16
	{0, 1, 0, 1, 0, 1, 0, 0}, // 17
	{0, 1, 0, 1, 0, 1, 0, 1}, // 18
	{0, 1, 0, 1, 0, 2},       // 19
	{0, 1, 0, 1, 0, 2, 0, 0}, // 20
	{0, 1, 0, 1, 0, 2, 0, 1}, // 21
	{0, 1, 0, 2},             // 22
	{0, 1, 0, 2, 0, 0},       // 23
	{0, 1, 0, 2, 0, 0, 0, 0}, // 24
	{0, 1, 0, 2, 0, 0, 0, 1}, // 25
	{0, 1, 0, 2, 0, 1},       // 26
	{0, 1, 0, 2, 0, 1, 0, 0}, // 27
	{0, 1, 0, 2, 0, 1, 0, 1}, // 28
	{0, 1, 0, 2, 0, 2},       // 29
	{0, 1, 0, 2, 0, 2, 0, 0}, // 30
	{0, 1, 0, 2, 0, 2, 0, 1}, // 31
	{0, 2},                   // 32
	{0, 2, 0},                // 33
	{0, 2, 0, 0},             // 34
}

func TestBasic(t *testing.T) {

	p := New()

	c := p.Copy()

	Convey("Get initial size", t, func() {
		So(p.Size(), ShouldEqual, 0)
	})

	Convey("Can insert 1st item", t, func() {
		val := c.Put(0, []byte("/foo"), []byte("FOO"))
		So(val, ShouldBeNil)
		So(c.Size(), ShouldEqual, 1)
		So(c.Get(0, []byte("/foo")).Val(), ShouldResemble, []byte("FOO"))
	})

	Convey("Can insert 2nd item", t, func() {
		val := c.Put(0, []byte("/bar"), []byte("BAR"))
		So(val, ShouldBeNil)
		So(c.Size(), ShouldEqual, 2)
		So(c.Get(0, []byte("/bar")).Val(), ShouldResemble, []byte("BAR"))
	})

	Convey("Can get nil item", t, func() {
		val := c.Get(0, []byte("/"))
		So(val, ShouldEqual, nil)
	})

	Convey("Can delete nil item", t, func() {
		val := c.Del(0, []byte("/foobar"))
		So(val, ShouldEqual, nil)
		So(c.Size(), ShouldEqual, 2)
		So(c.Get(0, []byte("/foobar")), ShouldEqual, nil)
	})

	Convey("Can delete 1st item", t, func() {
		val := c.Cut([]byte("/foo"))
		So(val.Val(), ShouldResemble, []byte("FOO"))
		So(c.Size(), ShouldEqual, 1)
		So(c.Get(0, []byte("/foo")), ShouldEqual, nil)
	})

	Convey("Can delete 2nd item", t, func() {
		val := c.Cut([]byte("/bar"))
		So(val.Val(), ShouldResemble, []byte("BAR"))
		So(c.Size(), ShouldEqual, 0)
		So(c.Get(0, []byte("/bar")), ShouldEqual, nil)
	})

	Convey("Can commit transaction", t, func() {
		n := c.Tree()
		So(n, ShouldNotBeNil)
		So(n.Size(), ShouldEqual, 0)
	})

}

func TestComplex(t *testing.T) {

	p := New()
	c := p.Copy()

	Convey("Can get empty `min`", t, func() {
		r := c.Root()
		k, v := r.Min()
		So(k, ShouldBeNil)
		So(v, ShouldBeNil)
	})

	Convey("Can get empty `max`", t, func() {
		r := c.Root()
		k, v := r.Max()
		So(k, ShouldBeNil)
		So(v, ShouldBeNil)
	})

	Convey("Can insert tree items", t, func() {
		for _, v := range s {
			c.Put(0, []byte(v), []byte(v))
		}
		So(c.Size(), ShouldEqual, 35)
		for i := len(s) - 1; i > 0; i-- {
			c.Put(0, []byte(s[i]), []byte(s[i]))
		}
		So(c.Size(), ShouldEqual, 35)
	})

	Convey("Can get proper `min`", t, func() {
		k, v := c.Root().Min()
		So(k, ShouldResemble, []byte("/some"))
		So(v.Get(0, Upto).Val(), ShouldResemble, []byte("/some"))
	})

	Convey("Can get proper `max`", t, func() {
		k, v := c.Root().Max()
		So(k, ShouldResemble, []byte("/zoo/some/path"))
		So(v.Get(0, Upto).Val(), ShouldResemble, []byte("/zoo/some/path"))
	})

	// ------------------------------------------------------------

	Convey("Can iterate tree items at `nil` with `walk`", t, func() {
		i := 0
		c.Root().Walk(nil, func(k []byte, v *List) (e bool) {
			i++
			return
		})
		So(i, ShouldEqual, 35)
	})

	Convey("Can iterate tree items at `/test/zen/s` with `walk`", t, func() {
		i := 0
		c.Root().Walk([]byte("/test/zen/s"), func(k []byte, v *List) (e bool) {
			i++
			return
		})
		So(i, ShouldEqual, 9)
	})

	Convey("Can iterate tree items at `/test/zen/sub` with `walk`", t, func() {
		i := 0
		c.Root().Walk([]byte("/test/zen/sub"), func(k []byte, v *List) (e bool) {
			i++
			return
		})
		So(i, ShouldEqual, 9)
	})

	Convey("Can iterate tree items at `/test/zen/sub-o` with `walk`", t, func() {
		i := 0
		c.Root().Walk([]byte("/test/zen/sub-o"), func(k []byte, v *List) (e bool) {
			i++
			return
		})
		So(i, ShouldEqual, 3)
	})

	Convey("Can iterate tree items at `/test/zen/sub-one` with `walk`", t, func() {
		i := 0
		c.Root().Walk([]byte("/test/zen/sub-one"), func(k []byte, v *List) (e bool) {
			i++
			return
		})
		So(i, ShouldEqual, 3)
	})

	Convey("Can iterate tree items at `/test/zen/sub` with `walk` and exit", t, func() {
		i := 0
		c.Root().Walk([]byte("/test/zen/sub"), func(k []byte, v *List) (e bool) {
			i++
			return true
		})
		So(i, ShouldEqual, 1)
	})

	// ------------------------------------------------------------

	Convey("Can iterate tree items at `/test/` with `subs`", t, func() {
		i := 0
		c.Root().Subs([]byte("/test/"), func(k []byte, v *List) (e bool) {
			i++
			return
		})
		So(i, ShouldEqual, 3)
	})

	Convey("Can iterate tree items at `/test/zen/s` with `subs`", t, func() {
		i := 0
		c.Root().Subs([]byte("/test/zen/s"), func(k []byte, v *List) (e bool) {
			i++
			return
		})
		So(i, ShouldEqual, 3)
	})

	Convey("Can iterate tree items at `/test/zen/sub` with `subs`", t, func() {
		i := 0
		c.Root().Subs([]byte("/test/zen/sub"), func(k []byte, v *List) (e bool) {
			i++
			return
		})
		So(i, ShouldEqual, 3)
	})

	Convey("Can iterate tree items at `/test/zen/sub-o` with `subs`", t, func() {
		i := 0
		c.Root().Subs([]byte("/test/zen/sub-t"), func(k []byte, v *List) (e bool) {
			i++
			return
		})
		So(i, ShouldEqual, 1)
	})

	Convey("Can iterate tree items at `/test/zen/sub-one` with `subs`", t, func() {
		i := 0
		c.Root().Subs([]byte("/test/zen/sub-one"), func(k []byte, v *List) (e bool) {
			i++
			return
		})
		So(i, ShouldEqual, 2)
	})

	Convey("Can iterate tree items at `/test/zen/sub` with `subs` and exit", t, func() {
		i := 0
		c.Root().Subs([]byte("/test/zen/sub"), func(k []byte, v *List) (e bool) {
			i++
			return true
		})
		So(i, ShouldEqual, 1)
	})

	// ------------------------------------------------------------

	Convey("Can iterate tree items at `nil` with `path`", t, func() {
		i := 0
		c.Root().Path(nil, func(k []byte, v *List) (e bool) {
			i++
			return
		})
		So(i, ShouldEqual, 0)
	})

	Convey("Can iterate tree items at `/test/zen/s` with `path`", t, func() {
		i := 0
		c.Root().Path([]byte("/test/zen/s"), func(k []byte, v *List) (e bool) {
			i++
			return
		})
		So(i, ShouldEqual, 2)
	})

	Convey("Can iterate tree items at `/test/zen/sub` with `path`", t, func() {
		i := 0
		c.Root().Path([]byte("/test/zen/sub"), func(k []byte, v *List) (e bool) {
			i++
			return
		})
		So(i, ShouldEqual, 2)
	})

	Convey("Can iterate tree items at `/test/zen/sub-o` with `path`", t, func() {
		i := 0
		c.Root().Path([]byte("/test/zen/sub-o"), func(k []byte, v *List) (e bool) {
			i++
			return
		})
		So(i, ShouldEqual, 2)
	})

	Convey("Can iterate tree items at `/test/zen/sub-one` with `path`", t, func() {
		i := 0
		c.Root().Path([]byte("/test/zen/sub-one"), func(k []byte, v *List) (e bool) {
			i++
			return
		})
		So(i, ShouldEqual, 3)
	})

	Convey("Can iterate tree items at `/test/zen/sub` with `path` and exit", t, func() {
		i := 0
		c.Root().Path([]byte("/test/zen/sub"), func(k []byte, v *List) (e bool) {
			i++
			return true
		})
		So(i, ShouldEqual, 1)
	})

}

func TestIritate(t *testing.T) {

	c := New().Copy()

	i := c.Cursor()

	Convey("Can iterate to the min with no items", t, func() {
		k, v := i.First()
		So(v, ShouldBeNil)
		So(k, ShouldBeNil)
	})

	Convey("Can iterate to the max with no items", t, func() {
		k, v := i.Last()
		So(v, ShouldBeNil)
		So(k, ShouldBeNil)
	})

	Convey("Can seek to a key with no items", t, func() {
		k, v := i.Seek([]byte(""))
		So(v, ShouldBeNil)
		So(k, ShouldBeNil)
	})

	Convey("Can seek to a key with no items", t, func() {
		k, v := i.Seek([]byte("/something"))
		So(v, ShouldBeNil)
		So(k, ShouldBeNil)
	})

}

func TestIterate(t *testing.T) {

	c := New().Copy()

	Convey("Can insert tree items", t, func() {
		for _, v := range s {
			c.Put(0, []byte(v), []byte(v))
		}
		So(c.Size(), ShouldEqual, 35)
	})

	i := c.Cursor()

	Convey("Can get iterator", t, func() {
		So(i, ShouldNotBeNil)
	})

	Convey("Prev with no seek returns nil", t, func() {
		k, v := i.Prev()
		So(k, ShouldBeNil)
		So(v, ShouldBeNil)
	})

	Convey("Next with no seek returns nil", t, func() {
		k, v := i.Next()
		So(k, ShouldBeNil)
		So(v, ShouldBeNil)
	})

	Convey("Can iterate to the min", t, func() {
		k, v := i.First()
		var t []int
		for _, q := range i.path {
			t = append(t, q.pos)
		}
		So(fmt.Sprint(t), ShouldEqual, fmt.Sprint(p[0]))
		So(k, ShouldResemble, []byte(s[0]))
		So(v.Get(0, Upto).Val(), ShouldResemble, []byte(s[0]))
	})

	Convey("Can iterate using `next`", t, func() {
		for j := 1; j < len(s); j++ {
			k, v := i.Next()
			var t []int
			for _, q := range i.path {
				t = append(t, q.pos)
			}
			So(fmt.Sprint(t), ShouldEqual, fmt.Sprint(p[j]))
			So(k, ShouldResemble, []byte(s[j]))
			So(v.Get(0, Upto).Val(), ShouldResemble, []byte(s[j]))
		}
	})

	Convey("Next item is nil and doesn't change cursor", t, func() {
		k, v := i.Next()
		So(k, ShouldBeNil)
		So(v, ShouldBeNil)
	})

	Convey("Can iterate to the max", t, func() {
		k, v := i.Last()
		var t []int
		for _, q := range i.path {
			t = append(t, q.pos)
		}
		So(fmt.Sprint(t), ShouldEqual, fmt.Sprint(p[len(p)-1]))
		So(k, ShouldResemble, []byte(s[len(p)-1]))
		So(v.Get(0, Upto).Val(), ShouldResemble, []byte(s[len(p)-1]))
	})

	Convey("Can iterate using `prev`", t, func() {
		for j := len(s) - 2; j >= 0; j-- {
			k, v := i.Prev()
			var t []int
			for _, q := range i.path {
				t = append(t, q.pos)
			}
			So(fmt.Sprint(t), ShouldEqual, fmt.Sprint(p[j]))
			So(k, ShouldResemble, []byte(s[j]))
			So(v.Get(0, Upto).Val(), ShouldResemble, []byte(s[j]))
		}
	})

	Convey("Prev item is nil and doesn't change cursor", t, func() {
		k, v := i.Prev()
		So(k, ShouldBeNil)
		So(v, ShouldBeNil)
	})

	Convey("Seek nonexistant nil", t, func() {
		k, v := i.Seek(nil)
		So(k, ShouldResemble, []byte(s[0]))
		So(v.Get(0, Upto).Val(), ShouldResemble, []byte(s[0]))
	})

	Convey("Seek nonexistant first byte", t, func() {
		k, v := i.Seek([]byte{0})
		So(k, ShouldResemble, []byte(s[0]))
		So(v.Get(0, Upto).Val(), ShouldResemble, []byte(s[0]))
	})

	Convey("Seek nonexistant first item", t, func() {
		k, v := i.Seek([]byte("/aaa"))
		So(k, ShouldResemble, []byte(s[0]))
		So(v.Get(0, Upto).Val(), ShouldResemble, []byte(s[0]))
	})

	Convey("Seek just over last item", t, func() {
		k, v := i.Seek([]byte("/zoo/some/path/-"))
		So(v, ShouldBeNil)
		So(k, ShouldBeNil)
	})

	Convey("Seek nonexistant last item", t, func() {
		k, v := i.Seek([]byte("/zzz"))
		So(v, ShouldBeNil)
		So(k, ShouldBeNil)
	})

	Convey("Seek nonexistant last byte", t, func() {
		k, v := i.Seek([]byte{255})
		So(v, ShouldBeNil)
		So(k, ShouldBeNil)
	})

	Convey("Seek half item is correct", t, func() {
		k, v := i.Seek([]byte(s[10][:len(s[10])-3]))
		var t []int
		for _, q := range i.path {
			t = append(t, q.pos)
		}
		So(fmt.Sprint(t), ShouldEqual, fmt.Sprint(p[10]))
		So(k, ShouldResemble, []byte(s[10]))
		So(v.Get(0, Upto).Val(), ShouldResemble, []byte(s[10]))
	})

	Convey("Seek full item is correct", t, func() {
		k, v := i.Seek([]byte(s[10]))
		var t []int
		for _, q := range i.path {
			t = append(t, q.pos)
		}
		So(fmt.Sprint(t), ShouldEqual, fmt.Sprint(p[10]))
		So(k, ShouldResemble, []byte(s[10]))
		So(v.Get(0, Upto).Val(), ShouldResemble, []byte(s[10]))
	})

	Convey("Seek overfull item is correct", t, func() {
		k, v := i.Seek([]byte(s[10] + "-"))
		var t []int
		for _, q := range i.path {
			t = append(t, q.pos)
		}
		So(fmt.Sprint(t), ShouldEqual, fmt.Sprint(p[11]))
		So(k, ShouldResemble, []byte(s[11]))
		So(v.Get(0, Upto).Val(), ShouldResemble, []byte(s[11]))
	})

	Convey("Seek finishing item is correct", t, func() {
		k, v := i.Seek([]byte("/test/zzz"))
		var t []int
		for _, q := range i.path {
			t = append(t, q.pos)
		}
		So(fmt.Sprint(t), ShouldEqual, fmt.Sprint(p[32]))
		So(k, ShouldResemble, []byte(s[32]))
		So(v.Get(0, Upto).Val(), ShouldResemble, []byte(s[32]))
	})

	Convey("Seek finalising item is correct", t, func() {
		k, v := i.Seek([]byte("/zoo/some/xxxx"))
		So(v, ShouldBeNil)
		So(k, ShouldBeNil)
	})

	Convey("Prev item after seek is correct", t, func() {
		i.Seek([]byte(s[10]))
		k, v := i.Prev()
		var t []int
		for _, q := range i.path {
			t = append(t, q.pos)
		}
		So(fmt.Sprint(t), ShouldEqual, fmt.Sprint(p[9]))
		So(k, ShouldResemble, []byte(s[9]))
		So(v.Get(0, Upto).Val(), ShouldResemble, []byte(s[9]))
	})

	Convey("Next item after seek is correct", t, func() {
		i.Seek([]byte(s[10]))
		k, v := i.Next()
		var t []int
		for _, q := range i.path {
			t = append(t, q.pos)
		}
		So(fmt.Sprint(t), ShouldEqual, fmt.Sprint(p[11]))
		So(k, ShouldResemble, []byte(s[11]))
		So(v.Get(0, Upto).Val(), ShouldResemble, []byte(s[11]))
	})

	Convey("FINAL", t, func() {
		i.Seek([]byte(s[10]))
		i.Del()
		k, v := i.Next()
		var t []int
		for _, q := range i.path {
			t = append(t, q.pos)
		}
		So(fmt.Sprint(t), ShouldEqual, fmt.Sprint(p[11]))
		So(k, ShouldResemble, []byte(s[11]))
		So(v.Get(0, Upto).Val(), ShouldResemble, []byte(s[11]))
	})

	Convey("FINAL", t, func() {
		var k []byte
		i.Seek([]byte(s[10]))
		i.Del()
		k, _ = i.Next()
		i.Del()
		k, _ = i.Next()
		i.Del()
		k, _ = i.Next()
		i.Del()
		k, _ = i.Next()
		i.Del()
		k, v := i.Next()
		var t []int
		for _, q := range i.path {
			t = append(t, q.pos)
		}
		So(fmt.Sprint(t), ShouldEqual, fmt.Sprint(p[15]))
		So(k, ShouldResemble, []byte(s[15]))
		So(v.Get(0, Upto).Val(), ShouldResemble, []byte(s[15]))
	})

}

func TestUpdate(t *testing.T) {

	c := New().Copy()

	Convey("Can insert 1st item", t, func() {
		val := c.Put(0, []byte("/test"), []byte("ONE"))
		So(val, ShouldBeNil)
		So(val, ShouldEqual, nil)
		So(c.Size(), ShouldEqual, 1)
		So(c.Get(0, []byte("/test")).Val(), ShouldResemble, []byte("ONE"))
	})

	Convey("Can insert 2nd item", t, func() {
		val := c.Put(0, []byte("/test"), []byte("TWO"))
		So(val, ShouldNotBeNil)
		// So(val.Val(), ShouldResemble, []byte("ONE"))
		So(c.Size(), ShouldEqual, 1)
		So(c.Get(0, []byte("/test")).Val(), ShouldResemble, []byte("TWO"))
	})

	Convey("Can insert 3rd item", t, func() {
		val := c.Put(0, []byte("/test"), []byte("TRE"))
		So(val, ShouldNotBeNil)
		// So(val.Val(), ShouldResemble, []byte("TWO"))
		So(c.Size(), ShouldEqual, 1)
		So(c.Get(0, []byte("/test")).Val(), ShouldResemble, []byte("TRE"))
	})

}

func TestDelete(t *testing.T) {

	c := New().Copy()

	Convey("Can insert 1st item", t, func() {
		val := c.Put(0, []byte("/test"), []byte("TEST"))
		So(val, ShouldBeNil)
		So(val, ShouldEqual, nil)
		So(c.Size(), ShouldEqual, 1)
		So(c.Get(0, []byte("/test")).Val(), ShouldResemble, []byte("TEST"))
	})

	Convey("Can delete 1st item", t, func() {
		val := c.Cut([]byte("/test"))
		So(val, ShouldNotBeNil)
		So(val.Val(), ShouldResemble, []byte("TEST"))
		So(c.Size(), ShouldEqual, 0)
		So(c.Get(0, []byte("/test")), ShouldBeNil)
	})

	Convey("Can delete 1st item", t, func() {
		val := c.Cut([]byte("/test"))
		So(val, ShouldBeNil)
		So(val, ShouldEqual, nil)
		So(c.Size(), ShouldEqual, 0)
		So(c.Get(0, []byte("/test")), ShouldBeNil)
	})

}

func TestVersion(t *testing.T) {

	c := New().Copy()

	Convey("Can insert a 1st versioned item", t, func() {
		val := c.Put(5, []byte("/one"), []byte("ONE"))
		So(val, ShouldBeNil)
		So(c.Size(), ShouldEqual, 1)
		So(c.Get(3, []byte("/one")), ShouldBeNil)
		So(c.Get(5, []byte("/one")).Val(), ShouldResemble, []byte("ONE"))
		So(c.Get(7, []byte("/one")).Val(), ShouldResemble, []byte("ONE"))
	})

	Convey("Can update a 1st versioned item", t, func() {
		val := c.Put(6, []byte("/one"), []byte("ONE-NEW"))
		So(val, ShouldNotBeNil)
		So(val.Val(), ShouldResemble, []byte("ONE"))
		So(c.Size(), ShouldEqual, 1)
		So(c.Get(3, []byte("/one")), ShouldBeNil)
		So(c.Get(5, []byte("/one")).Val(), ShouldResemble, []byte("ONE"))
		So(c.Get(7, []byte("/one")).Val(), ShouldResemble, []byte("ONE-NEW"))
	})

	Convey("Can iterate to the item", t, func() {
		i := c.Cursor()
		k, v := i.Seek([]byte("/one"))
		So(k, ShouldResemble, []byte("/one"))
		So(v.Get(m, Upto).Val(), ShouldResemble, []byte("ONE-NEW"))
		So(i.Here().Get(0, Upto), ShouldBeNil)
		So(i.Here().Get(1, Upto), ShouldBeNil)
		So(i.Here().Min().Val(), ShouldResemble, []byte("ONE"))
		So(i.Here().Max().Val(), ShouldResemble, []byte("ONE-NEW"))
		So(i.Here().Get(5, Upto).Ver(), ShouldEqual, 5)
		So(i.Here().Get(5, Upto).Val(), ShouldResemble, []byte("ONE"))
		So(i.Here().Get(6, Upto).Ver(), ShouldEqual, 6)
		So(i.Here().Get(6, Upto).Val(), ShouldResemble, []byte("ONE-NEW"))
		So(i.Here().Get(9, Upto).Ver(), ShouldEqual, 6)
		So(i.Here().Get(9, Upto).Val(), ShouldResemble, []byte("ONE-NEW"))
		So(i.Here().Get(m, Upto).Ver(), ShouldEqual, 6)
		So(i.Here().Get(m, Upto).Val(), ShouldResemble, []byte("ONE-NEW"))
	})

	Convey("Can iterate and walk over the item and exit", t, func() {
		var ts []uint64
		i := c.Cursor()
		i.Seek([]byte("/one"))
		i.Here().Walk(func(x *Item) (exit bool) {
			ts = append(ts, x.Ver())
			return true
		})
		So(ts, ShouldHaveLength, 1)
	})

	Convey("Can iterate and walk over the item and continue", t, func() {
		var ts []uint64
		i := c.Cursor()
		i.Seek([]byte("/one"))
		i.Here().Walk(func(x *Item) (exit bool) {
			ts = append(ts, x.Ver())
			return false
		})
		So(ts, ShouldHaveLength, 2)
	})

	Convey("Can remove a 1st versioned item", t, func() {
		val := c.Put(8, []byte("/one"), nil)
		So(val.Val(), ShouldNotBeNil)
		So(val.Val(), ShouldResemble, []byte("ONE-NEW"))
		So(c.Size(), ShouldEqual, 1)
		So(c.Get(0, []byte("/one")), ShouldBeNil)
		So(c.Get(3, []byte("/one")), ShouldBeNil)
		So(c.Get(5, []byte("/one")).Val(), ShouldResemble, []byte("ONE"))
		So(c.Get(7, []byte("/one")).Val(), ShouldResemble, []byte("ONE-NEW"))
		So(c.Get(9, []byte("/one")).Val(), ShouldBeNil)
	})

	Convey("Can iterate to the item once removed", t, func() {
		i := c.Cursor()
		k, v := i.Seek([]byte("/one"))
		So(k, ShouldResemble, []byte("/one"))
		So(v.Get(0, Upto), ShouldBeNil)
		So(i.Here().Get(0, Upto), ShouldBeNil)
		So(i.Here().Get(3, Upto), ShouldBeNil)
		So(i.Here().Get(5, Upto).Val(), ShouldResemble, []byte("ONE"))
		So(i.Here().Get(7, Upto).Val(), ShouldResemble, []byte("ONE-NEW"))
		So(i.Here().Get(9, Upto).Val(), ShouldResemble, []byte(nil))
	})

	Convey("Can insert a 2nd versioned item", t, func() {
		val := c.Put(5, []byte("/two"), []byte("TWO"))
		So(val, ShouldBeNil)
		So(c.Size(), ShouldEqual, 2)
		So(c.Get(3, []byte("/two")), ShouldBeNil)
		So(c.Get(5, []byte("/two")).Val(), ShouldResemble, []byte("TWO"))
		So(c.Get(7, []byte("/two")).Val(), ShouldResemble, []byte("TWO"))
	})

	Convey("Can update a 2nd versioned item", t, func() {
		val := c.Put(6, []byte("/two"), []byte("TWO-NEW"))
		So(val.Val(), ShouldNotBeNil)
		So(val.Val(), ShouldResemble, []byte("TWO"))
		So(c.Size(), ShouldEqual, 2)
		So(c.Get(3, []byte("/two")), ShouldBeNil)
		So(c.Get(5, []byte("/two")).Val(), ShouldResemble, []byte("TWO"))
		So(c.Get(7, []byte("/two")).Val(), ShouldResemble, []byte("TWO-NEW"))
	})

	Convey("Can iterate to the item", t, func() {
		i := c.Cursor()
		k, v := i.Seek([]byte("/two"))
		So(k, ShouldResemble, []byte("/two"))
		So(v.Get(m, Upto).Val(), ShouldResemble, []byte("TWO-NEW"))
		So(i.Here().Get(0, Upto), ShouldBeNil)
		So(i.Here().Get(1, Upto), ShouldBeNil)
		So(i.Here().Min().Val(), ShouldResemble, []byte("TWO"))
		So(i.Here().Max().Val(), ShouldResemble, []byte("TWO-NEW"))
		So(i.Here().Get(5, Upto).Ver(), ShouldEqual, 5)
		So(i.Here().Get(5, Upto).Val(), ShouldResemble, []byte("TWO"))
		So(i.Here().Get(6, Upto).Ver(), ShouldEqual, 6)
		So(i.Here().Get(6, Upto).Val(), ShouldResemble, []byte("TWO-NEW"))
		So(i.Here().Get(9, Upto).Ver(), ShouldEqual, 6)
		So(i.Here().Get(9, Upto).Val(), ShouldResemble, []byte("TWO-NEW"))
		So(i.Here().Get(m, Upto).Ver(), ShouldEqual, 6)
		So(i.Here().Get(m, Upto).Val(), ShouldResemble, []byte("TWO-NEW"))
	})

	Convey("Can iterate and walk over the item and exit", t, func() {
		var ts []uint64
		i := c.Cursor()
		i.Seek([]byte("/two"))
		i.Here().Walk(func(x *Item) (exit bool) {
			ts = append(ts, x.Ver())
			return true
		})
		So(ts, ShouldHaveLength, 1)
	})

	Convey("Can iterate and walk over the item and continue", t, func() {
		var ts []uint64
		i := c.Cursor()
		i.Seek([]byte("/two"))
		i.Here().Walk(func(x *Item) (exit bool) {
			ts = append(ts, x.Ver())
			return false
		})
		So(ts, ShouldHaveLength, 2)
	})

	Convey("Can remove a 2nd versioned item", t, func() {
		val := c.Put(8, []byte("/two"), nil)
		So(val.Val(), ShouldNotBeNil)
		So(val.Val(), ShouldResemble, []byte("TWO-NEW"))
		So(c.Size(), ShouldEqual, 2)
		So(c.Get(0, []byte("/two")), ShouldBeNil)
		So(c.Get(3, []byte("/two")), ShouldBeNil)
		So(c.Get(5, []byte("/two")).Val(), ShouldResemble, []byte("TWO"))
		So(c.Get(7, []byte("/two")).Val(), ShouldResemble, []byte("TWO-NEW"))
		So(c.Get(9, []byte("/two")).Val(), ShouldBeNil)
	})

	Convey("Can iterate to the item once removed", t, func() {
		i := c.Cursor()
		k, v := i.Seek([]byte("/two"))
		So(k, ShouldResemble, []byte("/two"))
		So(v.Get(0, Upto), ShouldBeNil)
		So(i.Here().Get(0, Upto), ShouldBeNil)
		So(i.Here().Get(3, Upto), ShouldBeNil)
		So(i.Here().Get(5, Upto).Val(), ShouldResemble, []byte("TWO"))
		So(i.Here().Get(7, Upto).Val(), ShouldResemble, []byte("TWO-NEW"))
		So(i.Here().Get(9, Upto).Val(), ShouldResemble, []byte(nil))
	})

	Convey("Can delete the latest version", t, func() {
		val := c.Del(5, []byte("/one"))
		So(val.Val(), ShouldNotBeNil)
		So(val.Val(), ShouldResemble, []byte("ONE"))
		So(c.Size(), ShouldEqual, 2)
		So(c.Get(3, []byte("/one")), ShouldBeNil)
		So(c.Get(5, []byte("/one")).Val(), ShouldBeNil)
		So(c.Get(7, []byte("/one")).Val(), ShouldResemble, []byte("ONE-NEW"))
	})

	Convey("Can delete invalid version", t, func() {
		val := c.Del(3, []byte("/one"))
		So(val.Val(), ShouldBeNil)
		So(c.Size(), ShouldEqual, 2)
		So(c.Get(3, []byte("/one")).Val(), ShouldBeNil)
		So(c.Get(5, []byte("/one")).Val(), ShouldBeNil)
		So(c.Get(7, []byte("/one")).Val(), ShouldResemble, []byte("ONE-NEW"))
	})

	Convey("Can delete whole key", t, func() {
		val := c.Cut([]byte("/one"))
		So(val.Val(), ShouldBeNil)
		So(c.Size(), ShouldEqual, 1)
		So(c.Get(3, []byte("/one")), ShouldBeNil)
		So(c.Get(5, []byte("/one")), ShouldBeNil)
		So(c.Get(7, []byte("/one")), ShouldBeNil)
	})

	Convey("Can delete whole key", t, func() {
		val := c.Cut([]byte("/two"))
		So(val.Val(), ShouldBeNil)
		So(c.Size(), ShouldEqual, 0)
		So(c.Get(3, []byte("/two")), ShouldBeNil)
		So(c.Get(5, []byte("/two")), ShouldBeNil)
		So(c.Get(7, []byte("/two")), ShouldBeNil)
	})

}
