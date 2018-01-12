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
	"github.com/abcum/syncr/gcs"
	"github.com/abcum/syncr/logr"
	"github.com/abcum/syncr/s3"
)

func (db *DB) locate(path string) (string, string) {

	// s3://bucket/path/to/file.db
	if strings.HasPrefix(path, "s3://") {
		return "s3", strings.TrimPrefix(path, "s3://")
	}

	// gcs://bucket/path/to/file.db
	if strings.HasPrefix(path, "gcs://") {
		return "gcs", strings.TrimPrefix(path, "gcs://")
	}

	// logr://path/to/folder/with/file.db
	if strings.HasPrefix(path, "logr://") {
		return "logr", strings.TrimPrefix(path, "logr://")
	}

	// file://path/to/file.db
	if strings.HasPrefix(path, "file://") {
		return "file", strings.TrimPrefix(path, "file://")
	}

	return "file", path

}

func (db *DB) syncer(path string) (syncr.Syncable, error) {

	if db.kind == "s3" {
		return s3.New(path, &s3.Options{
			MinSize: db.conf.SizePolicy,
		})
	}

	if db.kind == "gcs" {
		return gcs.New(path, &gcs.Options{
			MinSize: db.conf.SizePolicy,
		})
	}

	if db.kind == "logr" {
		return logr.New(path, &logr.Options{
			MaxSize: db.conf.SizePolicy,
		})
	}

	if db.kind == "file" {
		return file.New(path)
	}

	return nil, nil

}
