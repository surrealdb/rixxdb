// Copyright © 2019 Abcum Ltd
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
var x *Trie
var i *Item

func TestTrie(t *testing.T) {

	x = newTrie()

	Convey("Get with nothing in list", t, func() {
		So(x.Get(3), ShouldBeNil)
	})

	Convey("Can set 2nd item", t, func() {
		x.Put(2, []byte{2})
		i = x.Get(2)
		So(x.Min(), ShouldEqual, x.Get(2))
		So(x.Max(), ShouldEqual, x.Get(2))
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{2})
	})

	Convey("Can set 4th item", t, func() {
		x.Put(4, []byte{4})
		i = x.Get(4)
		So(x.Min(), ShouldEqual, x.Get(2))
		So(x.Max(), ShouldEqual, x.Get(4))
		So(i.Ver(), ShouldEqual, 4)
		So(i.Val(), ShouldResemble, []byte{4})
	})

	Convey("Can set 1st item", t, func() {
		x.Put(1, []byte{1})
		i = x.Get(1)
		So(x.Min(), ShouldEqual, x.Get(1))
		So(x.Max(), ShouldEqual, x.Get(4))
		So(i.Ver(), ShouldEqual, 1)
		So(i.Val(), ShouldResemble, []byte{1})
	})

	Convey("Can set 3rd item", t, func() {
		x.Put(3, []byte{3})
		i = x.Get(3)
		So(x.Min(), ShouldEqual, x.Get(1))
		So(x.Max(), ShouldEqual, x.Get(4))
		So(i.Ver(), ShouldEqual, 3)
		So(i.Val(), ShouldResemble, []byte{3})
	})

	Convey("Can set 5th item", t, func() {
		x.Put(5, []byte{5})
		i = x.Get(5)
		So(x.Min(), ShouldEqual, x.Get(1))
		So(x.Max(), ShouldEqual, x.Get(5))
		So(i.Ver(), ShouldEqual, 5)
		So(i.Val(), ShouldResemble, []byte{5})
	})

	Convey("Can get list size", t, func() {
		So(x.Len(), ShouldEqual, 5)
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can get prev item to 1", t, func() {
		i = x.Predecessor(1)
		So(i.Ver(), ShouldEqual, 1)
		So(i.Val(), ShouldResemble, []byte{1})
	})

	Convey("Can get prev item to 3", t, func() {
		i = x.Predecessor(3)
		So(i.Ver(), ShouldEqual, 3)
		So(i.Val(), ShouldResemble, []byte{3})
	})

	Convey("Can get next item to 3", t, func() {
		i = x.Successor(3)
		So(i.Ver(), ShouldEqual, 3)
		So(i.Val(), ShouldResemble, []byte{3})
	})

	Convey("Can get next item to 5", t, func() {
		i = x.Successor(5)
		So(i.Ver(), ShouldEqual, 5)
		So(i.Val(), ShouldResemble, []byte{5})
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can get upto item at 0", t, func() {
		So(x.Predecessor(0), ShouldBeNil)
	})

	Convey("Can get upto item at 1", t, func() {
		i = x.Predecessor(1)
		So(i.Ver(), ShouldEqual, 1)
		So(i.Val(), ShouldResemble, []byte{1})
		So(i, ShouldEqual, x.Get(1))
	})

	Convey("Can get upto item at 3", t, func() {
		i = x.Predecessor(3)
		So(i.Ver(), ShouldEqual, 3)
		So(i.Val(), ShouldResemble, []byte{3})
		So(i, ShouldEqual, x.Get(3))
	})

	Convey("Can get upto item at 5", t, func() {
		i = x.Predecessor(5)
		So(i.Ver(), ShouldEqual, 5)
		So(i.Val(), ShouldResemble, []byte{5})
		So(i, ShouldEqual, x.Get(5))
	})

	Convey("Can get upto item at 7", t, func() {
		i = x.Predecessor(7)
		So(i.Ver(), ShouldEqual, 5)
		So(i.Val(), ShouldResemble, []byte{5})
		So(i, ShouldEqual, x.Get(5))
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

	Convey("Can get range of items", t, func() {
		var items []*Item
		x.Rng(3, 5, func(i *Item) bool {
			items = append(items, i)
			return false
		})
		So(len(items), ShouldEqual, 2)
		So(items[0], ShouldEqual, x.Get(3))
		So(items[1], ShouldEqual, x.Get(4))
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can delete 1st item", t, func() {
		i = x.Del(1)
		So(i.Ver(), ShouldEqual, 1)
		So(i.Val(), ShouldResemble, []byte{1})
	})

	Convey("Can not get deleted item", t, func() {
		i = x.Get(1)
		So(i, ShouldBeNil)
	})

	Convey("Can get minimum item", t, func() {
		i = x.Min()
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{2})
		So(i, ShouldEqual, x.Get(2))
	})

	Convey("Can get maximum item", t, func() {
		i = x.Max()
		So(i.Ver(), ShouldEqual, 5)
		So(i.Val(), ShouldResemble, []byte{5})
		So(i, ShouldEqual, x.Get(5))
	})

	Convey("Can get list size", t, func() {
		So(x.Len(), ShouldEqual, 4)
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can delete 5th item", t, func() {
		i = x.Del(5)
		So(i.Ver(), ShouldEqual, 5)
		So(i.Val(), ShouldResemble, []byte{5})
	})

	Convey("Can not get deleted item", t, func() {
		i = x.Get(5)
		So(i, ShouldBeNil)
	})

	Convey("Can get minimum item", t, func() {
		i = x.Min()
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{2})
		So(i, ShouldEqual, x.Get(2))
	})

	Convey("Can get maximum item", t, func() {
		i = x.Max()
		So(i.Ver(), ShouldEqual, 4)
		So(i.Val(), ShouldResemble, []byte{4})
		So(i, ShouldEqual, x.Get(4))
	})

	Convey("Can get list size", t, func() {
		So(x.Len(), ShouldEqual, 3)
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can delete 3rd item", t, func() {
		i = x.Del(3)
		So(i.Ver(), ShouldEqual, 3)
		So(i.Val(), ShouldResemble, []byte{3})
	})

	Convey("Can not get deleted item", t, func() {
		i = x.Get(3)
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
		i = x.Del(2)
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{2})
	})

	Convey("Can not get deleted item", t, func() {
		i = x.Get(2)
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
		i = x.Del(4)
		So(i.Ver(), ShouldEqual, 4)
		So(i.Val(), ShouldResemble, []byte{4})
	})

	Convey("Can not get deleted item", t, func() {
		i = x.Get(4)
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
		i = x.Get(2)
		So(x.Min(), ShouldEqual, x.Get(2))
		So(x.Max(), ShouldEqual, x.Get(2))
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{2})
	})

	Convey("Can set 4th item", t, func() {
		x.Put(4, []byte{4})
		i = x.Get(4)
		So(x.Min(), ShouldEqual, x.Get(2))
		So(x.Max(), ShouldEqual, x.Get(4))
		So(i.Ver(), ShouldEqual, 4)
		So(i.Val(), ShouldResemble, []byte{4})
	})

	Convey("Can replace 2nd item", t, func() {
		i = x.Put(2, []byte{'R'})
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{2})
		i = x.Get(2)
		So(i.Ver(), ShouldEqual, 2)
		So(i.Val(), ShouldResemble, []byte{'R'})
	})

	Convey("Can replace 4th item", t, func() {
		i = x.Put(4, []byte{'R'})
		So(i.Ver(), ShouldEqual, 4)
		So(i.Val(), ShouldResemble, []byte{4})
		i = x.Get(4)
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

	Convey("Can walk through the list and exit", t, func() {
		var items []*Item
		x.Walk(func(i *Item) bool {
			items = append(items, i)
			return true
		})
		So(len(items), ShouldEqual, 1)
		So(items[0], ShouldEqual, x.Get(2))
	})

	Convey("Can walk through the list without exiting", t, func() {
		var items []*Item
		x.Walk(func(i *Item) bool {
			items = append(items, i)
			return false
		})
		So(len(items), ShouldEqual, 2)
		So(items[0], ShouldEqual, x.Get(2))
		So(items[1], ShouldEqual, x.Get(4))
	})

	// ----------------------------------------
	// ----------------------------------------
	// ----------------------------------------

	Convey("------------------------------", t, nil)

	Convey("Can clear the list", t, func() {
		x.Clr()
		So(x.Len(), ShouldEqual, 0)
	})

}
