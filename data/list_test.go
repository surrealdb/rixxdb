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
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var c int
var x *List
var i *Item

func TestMain(t *testing.T) {

	x = new(List)

	Convey("Get with nothing in list", t, func() {
		So(x.Get(3, Exact), ShouldBeNil)
	})

	Convey("Can set 2nd item", t, func() {
		x.Put(2, []byte{2})
		i = x.Get(2, Exact)
		So(x.Min(), ShouldEqual, x.Get(2, Exact))
		So(x.Max(), ShouldEqual, x.Get(2, Exact))
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{2})
		So(i.Prev(), ShouldEqual, nil)
		So(i.Next(), ShouldEqual, nil)
	})

	Convey("Can set 4th item", t, func() {
		x.Put(4, []byte{4})
		i = x.Get(4, Exact)
		So(x.Min(), ShouldEqual, x.Get(2, Exact))
		So(x.Max(), ShouldEqual, x.Get(4, Exact))
		So(i.Ver(), ShouldEqual, 4)
		So(i.Val(), ShouldResemble, []byte{4})
		So(i.Prev().Val(), ShouldResemble, []byte{2})
		So(i.Next(), ShouldEqual, nil)
	})

	Convey("Can set 1st item", t, func() {
		x.Put(1, []byte{1})
		i = x.Get(1, Exact)
		So(x.Min(), ShouldEqual, x.Get(1, Exact))
		So(x.Max(), ShouldEqual, x.Get(4, Exact))
		So(i.Ver(), ShouldEqual, 1)
		So(i.Val(), ShouldResemble, []byte{1})
		So(i.Prev(), ShouldEqual, nil)
		So(i.Next().Val(), ShouldResemble, []byte{2})
	})

	Convey("Can set 3rd item", t, func() {
		x.Put(3, []byte{3})
		i = x.Get(3, Exact)
		So(x.Min(), ShouldEqual, x.Get(1, Exact))
		So(x.Max(), ShouldEqual, x.Get(4, Exact))
		So(i.Ver(), ShouldEqual, 3)
		So(i.Val(), ShouldResemble, []byte{3})
		So(i.Prev().Val(), ShouldResemble, []byte{2})
		So(i.Next().Val(), ShouldResemble, []byte{4})
	})

	Convey("Can set 5th item", t, func() {
		x.Put(5, []byte{5})
		i = x.Get(5, Exact)
		So(x.Min(), ShouldEqual, x.Get(1, Exact))
		So(x.Max(), ShouldEqual, x.Get(5, Exact))
		So(i.Ver(), ShouldEqual, 5)
		So(i.Val(), ShouldResemble, []byte{5})
		So(i.Prev().Val(), ShouldResemble, []byte{4})
		So(i.Next(), ShouldEqual, nil)
	})

	Convey("Can get list size", t, func() {
		So(x.Len(), ShouldEqual, 5)
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can get prev item to 1", t, func() {
		So(x.Get(1, Prev), ShouldBeNil)
	})

	Convey("Can get prev item to 3", t, func() {
		i = x.Get(3, Prev)
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{2})
	})

	Convey("Can get next item to 3", t, func() {
		i = x.Get(3, Next)
		So(i.Ver(), ShouldEqual, 4)
		So(i.Val(), ShouldResemble, []byte{4})
	})

	Convey("Can get next item to 5", t, func() {
		So(x.Get(5, Next), ShouldBeNil)
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can get upto item at 0", t, func() {
		So(x.Get(0, Upto), ShouldBeNil)
	})

	Convey("Can get upto item at 1", t, func() {
		i = x.Get(1, Upto)
		So(i.Ver(), ShouldEqual, 1)
		So(i.Val(), ShouldResemble, []byte{1})
		So(i, ShouldEqual, x.Get(1, Exact))
	})

	Convey("Can get upto item at 3", t, func() {
		i = x.Get(3, Upto)
		So(i.Ver(), ShouldEqual, 3)
		So(i.Val(), ShouldResemble, []byte{3})
		So(i, ShouldEqual, x.Get(3, Exact))
	})

	Convey("Can get upto item at 5", t, func() {
		i = x.Get(5, Upto)
		So(i.Ver(), ShouldEqual, 5)
		So(i.Val(), ShouldResemble, []byte{5})
		So(i, ShouldEqual, x.Get(5, Exact))
	})

	Convey("Can get upto item at 7", t, func() {
		i = x.Get(7, Upto)
		So(i.Ver(), ShouldEqual, 5)
		So(i.Val(), ShouldResemble, []byte{5})
		So(i, ShouldEqual, x.Get(5, Exact))
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can get minimum item", t, func() {
		i = x.Min()
		So(i.Ver(), ShouldEqual, 1)
		So(i.Val(), ShouldResemble, []byte{1})
	})

	Convey("Can get maximum item", t, func() {
		i = x.Max()
		So(i.Ver(), ShouldEqual, 5)
		So(i.Val(), ShouldResemble, []byte{5})
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can get nearest item", t, func() {
		So(x.Get(0, Nearest).Ver(), ShouldEqual, 1)
		So(x.Get(1, Nearest).Ver(), ShouldEqual, 1)
		So(x.Get(3, Nearest).Ver(), ShouldEqual, 3)
		So(x.Get(5, Nearest).Ver(), ShouldEqual, 5)
		So(x.Get(10, Nearest).Ver(), ShouldEqual, 5)
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can get range of items", t, func() {
		var items []*Item
		x.Rng(3, 5, func(i *Item) bool {
			items = append(items, i)
			return false
		})
		So(len(items), ShouldEqual, 2)
		So(items[0], ShouldEqual, x.Get(3, Exact))
		So(items[1], ShouldEqual, x.Get(4, Exact))
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can delete 1st item", t, func() {
		i = x.Del(1, Exact)
		So(i.Ver(), ShouldEqual, 1)
		So(i.Val(), ShouldResemble, []byte{1})
		So(i.Prev(), ShouldEqual, nil)
		So(i.Next(), ShouldEqual, nil)
	})

	Convey("Can not get deleted item", t, func() {
		i = x.Get(1, Exact)
		So(i, ShouldBeNil)
	})

	Convey("Can get minimum item", t, func() {
		i = x.Min()
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{2})
		So(i, ShouldEqual, x.Get(2, Exact))
	})

	Convey("Can get maximum item", t, func() {
		i = x.Max()
		So(i.Ver(), ShouldEqual, 5)
		So(i.Val(), ShouldResemble, []byte{5})
		So(i, ShouldEqual, x.Get(5, Exact))
	})

	Convey("Can get list size", t, func() {
		So(x.Len(), ShouldEqual, 4)
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can delete 5th item", t, func() {
		i = x.Del(5, Exact)
		So(i.Ver(), ShouldEqual, 5)
		So(i.Val(), ShouldResemble, []byte{5})
		So(i.Prev(), ShouldEqual, nil)
		So(i.Next(), ShouldEqual, nil)
	})

	Convey("Can not get deleted item", t, func() {
		i = x.Get(5, Exact)
		So(i, ShouldBeNil)
	})

	Convey("Can get minimum item", t, func() {
		i = x.Min()
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{2})
		So(i, ShouldEqual, x.Get(2, Exact))
	})

	Convey("Can get maximum item", t, func() {
		i = x.Max()
		So(i.Ver(), ShouldEqual, 4)
		So(i.Val(), ShouldResemble, []byte{4})
		So(i, ShouldEqual, x.Get(4, Exact))
	})

	Convey("Can get list size", t, func() {
		So(x.Len(), ShouldEqual, 3)
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can delete 3rd item", t, func() {
		i = x.Del(3, Exact)
		So(i.Ver(), ShouldEqual, 3)
		So(i.Val(), ShouldResemble, []byte{3})
		So(i.Prev(), ShouldEqual, nil)
		So(i.Next(), ShouldEqual, nil)
	})

	Convey("Can not get deleted item", t, func() {
		i = x.Get(3, Exact)
		So(i, ShouldBeNil)
	})

	Convey("Can get minimum item", t, func() {
		i = x.Min()
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{2})
	})

	Convey("Can get maximum item", t, func() {
		i = x.Max()
		So(i.Ver(), ShouldEqual, 4)
		So(i.Val(), ShouldResemble, []byte{4})
	})

	Convey("Can get list size", t, func() {
		So(x.Len(), ShouldEqual, 2)
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can delete 2nd item", t, func() {
		i = x.Del(2, Exact)
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{2})
		So(i.Prev(), ShouldEqual, nil)
		So(i.Next(), ShouldEqual, nil)
	})

	Convey("Can not get deleted item", t, func() {
		i = x.Get(2, Exact)
		So(i, ShouldBeNil)
	})

	Convey("Can get minimum item", t, func() {
		i = x.Min()
		So(i.Ver(), ShouldEqual, 4)
		So(i.Val(), ShouldResemble, []byte{4})
	})

	Convey("Can get maximum item", t, func() {
		i = x.Max()
		So(i.Ver(), ShouldEqual, 4)
		So(i.Val(), ShouldResemble, []byte{4})
	})

	Convey("Can get list size", t, func() {
		So(x.Len(), ShouldEqual, 1)
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can delete 4th item", t, func() {
		i = x.Del(4, Exact)
		So(i.Ver(), ShouldEqual, 4)
		So(i.Val(), ShouldResemble, []byte{4})
		So(i.Prev(), ShouldEqual, nil)
		So(i.Next(), ShouldEqual, nil)
	})

	Convey("Can not get deleted item", t, func() {
		i = x.Get(4, Exact)
		So(i, ShouldBeNil)
	})

	Convey("Can get minimum item", t, func() {
		i = x.Min()
		So(i, ShouldBeNil)
	})

	Convey("Can get maximum item", t, func() {
		i = x.Max()
		So(i, ShouldBeNil)
	})

	Convey("Can get list size", t, func() {
		So(x.Len(), ShouldEqual, 0)
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can set 2nd item", t, func() {
		x.Put(2, []byte{2})
		i = x.Get(2, Exact)
		So(x.Min(), ShouldEqual, x.Get(2, Exact))
		So(x.Max(), ShouldEqual, x.Get(2, Exact))
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{2})
	})

	Convey("Can set 4th item", t, func() {
		x.Put(4, []byte{4})
		i = x.Get(4, Exact)
		So(x.Min(), ShouldEqual, x.Get(2, Exact))
		So(x.Max(), ShouldEqual, x.Get(4, Exact))
		So(i.Ver(), ShouldEqual, 4)
		So(i.Val(), ShouldResemble, []byte{4})
	})

	Convey("Can replace 2nd item", t, func() {
		i = x.Put(2, []byte{'R'})
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{2})
		i = x.Get(2, Exact)
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{'R'})
	})

	Convey("Can replace 4th item", t, func() {
		i = x.Put(4, []byte{'R'})
		So(i.Ver(), ShouldEqual, 4)
		So(i.Val(), ShouldResemble, []byte{4})
		i = x.Get(4, Exact)
		So(i.Ver(), ShouldEqual, 4)
		So(i.Val(), ShouldResemble, []byte{'R'})
	})

	Convey("Can get list size", t, func() {
		So(x.Len(), ShouldEqual, 2)
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can reset 2nd item", t, func() {
		i = x.Get(2, Exact)
		i.Set([]byte{'T'})
		So(i.Val(), ShouldResemble, []byte{'T'})
		So(x.Get(2, Exact).Val(), ShouldResemble, []byte{'T'})
	})

	Convey("Can reset 4th item", t, func() {
		i = x.Get(4, Exact)
		i.Set([]byte{'F'})
		So(i.Val(), ShouldResemble, []byte{'F'})
		So(x.Get(4, Exact).Val(), ShouldResemble, []byte{'F'})
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can walk through the list and exit", t, func() {
		var items []*Item
		x.Walk(func(i *Item) bool {
			items = append(items, i)
			return true
		})
		So(len(items), ShouldEqual, 1)
		So(items[0], ShouldEqual, x.Get(2, Exact))
	})

	Convey("Can walk through the list without exiting", t, func() {
		var items []*Item
		x.Walk(func(i *Item) bool {
			items = append(items, i)
			return false
		})
		So(len(items), ShouldEqual, 2)
		So(items[0], ShouldEqual, x.Get(2, Exact))
		So(items[1], ShouldEqual, x.Get(4, Exact))
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can insert some items", t, func() {
		x.Put(1, []byte{1})
		x.Put(2, []byte{2})
		x.Put(3, []byte{3})
		x.Put(4, []byte{4})
		x.Put(5, []byte{5})
		So(x.Len(), ShouldEqual, 5)
	})

	Convey("Can expire upto 3rd item", t, func() {
		i = x.Exp(3, Exact)
		So(x.Len(), ShouldEqual, 2)
		So(i.Ver(), ShouldEqual, 3)
		So(i.Val(), ShouldResemble, []byte{3})
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can clear the list", t, func() {
		x.Clr()
		So(x.Len(), ShouldEqual, 0)
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can insert some items", t, func() {
		x.Put(1, []byte{1})
		x.Put(2, []byte{2})
		x.Put(3, []byte{3})
		x.Put(4, []byte{4})
		x.Put(5, []byte{5})
		So(x.Len(), ShouldEqual, 5)
	})

	Convey("Can self delete 3rd item", t, func() {
		i = x.Get(3, Exact)
		So(i.Del(), ShouldEqual, i)
		So(x.Len(), ShouldEqual, 4)
	})

	Convey("Can self delete 1st item", t, func() {
		i = x.Get(1, Exact)
		So(i.Del(), ShouldEqual, i)
		So(x.Len(), ShouldEqual, 3)
	})

	Convey("Can self delete 5th item", t, func() {
		i = x.Get(5, Exact)
		So(i.Del(), ShouldEqual, i)
		So(x.Len(), ShouldEqual, 2)
	})

	Convey("Can self delete 2nd item", t, func() {
		i = x.Get(2, Exact)
		So(i.Del(), ShouldEqual, i)
		So(x.Len(), ShouldEqual, 1)
	})

	Convey("Can self delete 4th item", t, func() {
		i = x.Get(4, Exact)
		So(i.Del(), ShouldEqual, i)
		So(x.Len(), ShouldEqual, 0)
	})

}
