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
	"strings"

	"github.com/abcum/syncr"
	"github.com/abcum/syncr/file"
	"github.com/abcum/syncr/logr"
)

func (db *DB) locate(path string) (string, string) {

	// file://path/to/file.db
	if strings.HasPrefix(path, "file://") {
		return "file", strings.TrimPrefix(path, "file://")
	}

	// logr://path/to/folder/with/file.db
	if strings.HasPrefix(path, "logr://") {
		return "logr", strings.TrimPrefix(path, "logr://")
	}

	return "file", path

}

func (db *DB) syncer(path string) (syncr.Syncable, error) {

	if db.kind == "file" {
		return file.New(path)
	}

	if db.kind == "logr" {
		return logr.New(path, &logr.Options{
			MaxSize: db.conf.SizePolicy,
		})
	}

	return nil, nil

}
