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

package rixxdb

import (
	"fmt"
	"os"
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var test = []byte("test")

func TestNone(t *testing.T) {
	Convey("No persistence", t, func() {
		db, err := Open("memory", &Config{})
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)
		fullTests(db)
		So(db.Close(), ShouldBeNil)
	})
}

func TestPath(t *testing.T) {
	Convey("Path persistence", t, func() {
		db, err := Open("test.db", &Config{})
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)
		fullTests(db)
		os.RemoveAll("test.db")
		So(db.Close(), ShouldBeNil)
	})
}

func TestFile(t *testing.T) {
	Convey("File persistence", t, func() {
		db, err := Open("file://test.db", &Config{})
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)
		fullTests(db)
		os.RemoveAll("test.db")
		So(db.Close(), ShouldBeNil)
	})
}

func TestLogr(t *testing.T) {
	Convey("Logr persistence", t, func() {
		db, err := Open("logr://test/test.db", &Config{})
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)
		fullTests(db)
		os.RemoveAll("test")
		So(db.Close(), ShouldBeNil)
	})
}

func fullTests(db *DB) {

	var tx *TX
	var kv *KV
	var ok bool
	var err error

	Convey("Check transaction errors", func() {

		tx, err = db.Begin(false)

		// Test writing to a read transaction

		_, err = tx.Del(0, test)
		So(err, ShouldEqual, ErrTxNotWritable)
		_, err = tx.DelC(0, test, nil)
		So(err, ShouldEqual, ErrTxNotWritable)
		_, err = tx.DelL(0, test)
		So(err, ShouldEqual, ErrTxNotWritable)
		_, err = tx.DelP(0, test, 0)
		So(err, ShouldEqual, ErrTxNotWritable)
		_, err = tx.DelR(0, test, test, 0)
		So(err, ShouldEqual, ErrTxNotWritable)

		_, err = tx.Put(0, test, test)
		So(err, ShouldEqual, ErrTxNotWritable)
		_, err = tx.PutC(0, test, test, nil)
		So(err, ShouldEqual, ErrTxNotWritable)
		_, err = tx.PutL(0, test, test)
		So(err, ShouldEqual, ErrTxNotWritable)
		_, err = tx.PutP(0, test, test, 0)
		So(err, ShouldEqual, ErrTxNotWritable)
		_, err = tx.PutR(0, test, test, test, 0)
		So(err, ShouldEqual, ErrTxNotWritable)

		err = tx.Commit()
		So(err, ShouldEqual, ErrTxNotWritable)

		// Try altering a closed transaction

		err = tx.Cancel()
		So(err, ShouldEqual, ErrTxClosed)

		err = tx.Commit()
		So(err, ShouldEqual, ErrTxClosed)

		_, err = tx.Get(0, test)
		So(err, ShouldEqual, ErrTxClosed)
		_, err = tx.GetL(0, test)
		So(err, ShouldEqual, ErrTxClosed)
		_, err = tx.GetP(0, test, 0)
		So(err, ShouldEqual, ErrTxClosed)
		_, err = tx.GetR(0, test, test, 0)
		So(err, ShouldEqual, ErrTxClosed)

		_, err = tx.Del(0, test)
		So(err, ShouldEqual, ErrTxClosed)
		_, err = tx.DelC(0, test, nil)
		So(err, ShouldEqual, ErrTxClosed)
		_, err = tx.DelL(0, test)
		So(err, ShouldEqual, ErrTxClosed)
		_, err = tx.DelP(0, test, 0)
		So(err, ShouldEqual, ErrTxClosed)
		_, err = tx.DelR(0, test, test, 0)
		So(err, ShouldEqual, ErrTxClosed)

		_, err = tx.Put(0, test, test)
		So(err, ShouldEqual, ErrTxClosed)
		_, err = tx.PutC(0, test, test, nil)
		So(err, ShouldEqual, ErrTxClosed)
		_, err = tx.PutL(0, test, test)
		So(err, ShouldEqual, ErrTxClosed)
		_, err = tx.PutP(0, test, test, 0)
		So(err, ShouldEqual, ErrTxClosed)
		_, err = tx.PutR(0, test, test, test, 0)
		So(err, ShouldEqual, ErrTxClosed)

		// Try passing invalid arguments

		tx, err = db.Begin(true)

		_, err = tx.Get(All, test)
		So(err, ShouldEqual, ErrTxVersionNotSupported)
		_, err = tx.GetL(All, test)
		So(err, ShouldEqual, ErrTxVersionNotSupported)
		_, err = tx.GetP(All, test, 0)
		So(err, ShouldEqual, ErrTxVersionNotSupported)
		_, err = tx.GetR(All, test, test, 0)
		So(err, ShouldEqual, ErrTxVersionNotSupported)

		_, err = tx.Del(0, nil)
		So(err, ShouldEqual, ErrTxKeyCanNotBeNil)
		_, err = tx.DelC(0, nil, nil)
		So(err, ShouldEqual, ErrTxKeyCanNotBeNil)
		_, err = tx.DelL(0, nil)
		So(err, ShouldEqual, ErrTxKeyCanNotBeNil)
		_, err = tx.DelP(0, nil, 0)
		So(err, ShouldEqual, ErrTxKeyCanNotBeNil)
		_, err = tx.DelR(0, nil, test, 0)
		So(err, ShouldEqual, ErrTxKeyCanNotBeNil)

		_, err = tx.Put(0, nil, test)
		So(err, ShouldEqual, ErrTxKeyCanNotBeNil)
		_, err = tx.Put(All, test, test)
		So(err, ShouldEqual, ErrTxVersionNotSupported)
		_, err = tx.PutC(0, nil, test, nil)
		So(err, ShouldEqual, ErrTxKeyCanNotBeNil)
		_, err = tx.PutC(All, test, test, nil)
		So(err, ShouldEqual, ErrTxVersionNotSupported)
		_, err = tx.PutL(0, nil, test)
		So(err, ShouldEqual, ErrTxKeyCanNotBeNil)
		_, err = tx.PutL(All, test, test)
		So(err, ShouldEqual, ErrTxVersionNotSupported)
		_, err = tx.PutP(0, nil, test, 0)
		So(err, ShouldEqual, ErrTxKeyCanNotBeNil)
		_, err = tx.PutP(All, test, test, 0)
		So(err, ShouldEqual, ErrTxVersionNotSupported)
		_, err = tx.PutR(0, nil, test, test, 0)
		So(err, ShouldEqual, ErrTxKeyCanNotBeNil)
		_, err = tx.PutR(All, test, test, test, 0)
		So(err, ShouldEqual, ErrTxVersionNotSupported)

		tx.Cancel()

		// Test managed transaction errors

		db.View(func(tx *TX) error {
			err = tx.Cancel()
			So(err, ShouldEqual, ErrTxNotEditable)
			err = tx.Commit()
			So(err, ShouldEqual, ErrTxNotEditable)
			return nil
		})

		db.Update(func(tx *TX) error {
			err = tx.Cancel()
			So(err, ShouldEqual, ErrTxNotEditable)
			err = tx.Commit()
			So(err, ShouldEqual, ErrTxNotEditable)
			return nil
		})

	})

	Convey("Check manual transactions", func() {

		Convey("Write key and cancel transaction", func() {

			tx, err = db.Begin(true)
			So(err, ShouldBeNil)
			So(tx, ShouldNotBeNil)

			ok = tx.Closed()
			So(ok, ShouldBeFalse)

			kv, err = tx.Put(0, []byte("test"), []byte("tester"))
			So(err, ShouldBeNil)
			So(kv, ShouldNotBeNil)

			kv, err = tx.Get(0, []byte("test"))
			So(err, ShouldBeNil)
			So(kv, ShouldNotBeNil)
			So(kv.Exi(), ShouldBeTrue)

			err = tx.Cancel()
			So(err, ShouldBeNil)

			ok = tx.Closed()
			So(ok, ShouldBeTrue)

			tx, err = db.Begin(false)
			So(err, ShouldBeNil)
			So(tx, ShouldNotBeNil)

			kv, err = tx.Get(0, []byte("test"))
			So(err, ShouldBeNil)
			So(kv, ShouldNotBeNil)
			So(kv.Exi(), ShouldBeFalse)

			err = tx.Cancel()
			So(err, ShouldBeNil)

		})

		Convey("Write key and commit transaction", func() {

			tx, err = db.Begin(true)
			So(err, ShouldBeNil)
			So(tx, ShouldNotBeNil)

			ok = tx.Closed()
			So(ok, ShouldBeFalse)

			kv, err = tx.Put(0, []byte("test"), []byte("tester"))
			So(err, ShouldBeNil)
			So(kv, ShouldNotBeNil)

			kv, err = tx.Get(0, []byte("test"))
			So(err, ShouldBeNil)
			So(kv, ShouldNotBeNil)
			So(kv.Exi(), ShouldBeTrue)

			err = tx.Commit()
			So(err, ShouldBeNil)

			ok = tx.Closed()
			So(ok, ShouldBeTrue)

			tx, err = db.Begin(false)
			So(err, ShouldBeNil)
			So(tx, ShouldNotBeNil)

			kv, err = tx.Get(0, []byte("test"))
			So(err, ShouldBeNil)
			So(kv, ShouldNotBeNil)
			So(kv.Exi(), ShouldBeTrue)
			So(kv.Ver(), ShouldEqual, 0)
			So(kv.Key(), ShouldResemble, []byte("test"))
			So(kv.Val(), ShouldResemble, []byte("tester"))

			err = tx.Cancel()
			So(err, ShouldBeNil)

		})

		Convey("Write many keys concurrently", func() {

			max := 100

			tx, err = db.Begin(true)
			So(err, ShouldBeNil)
			So(tx, ShouldNotBeNil)

			ok = tx.Closed()
			So(ok, ShouldBeFalse)

			var w sync.WaitGroup

			w.Add(3)

			go func() {
				for i := 1; i < max; i++ {
					tx.Put(0, []byte(fmt.Sprint(i)), []byte(fmt.Sprint(i)))
				}
				w.Done()
			}()

			go func() {
				for i := 1; i < max; i++ {
					tx.Put(0, []byte(fmt.Sprint(i)), []byte(fmt.Sprint(i)))
				}
				w.Done()
			}()

			go func() {
				for i := 1; i < max; i++ {
					tx.Put(0, []byte(fmt.Sprint(i)), []byte(fmt.Sprint(i)))
				}
				w.Done()
			}()

			w.Wait()

			err = tx.Commit()
			So(err, ShouldBeNil)

			tx, err = db.Begin(false)
			So(err, ShouldBeNil)
			So(tx, ShouldNotBeNil)

			for i := 1; i < max; i++ {
				kv, err = tx.Get(0, []byte(fmt.Sprint(i)))
				So(err, ShouldBeNil)
				So(kv, ShouldNotBeNil)
				So(kv.Exi(), ShouldBeTrue)
				So(kv.Val(), ShouldResemble, []byte(fmt.Sprint(i)))
			}

			err = tx.Cancel()
			So(err, ShouldBeNil)

		})

	})

	Convey("Check managed transactions", func() {

	})

}
