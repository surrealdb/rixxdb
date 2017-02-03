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

func persist(path string, conf *Config) (syncr.Syncable, error) {

	// s3://user:pass@bucket/path/to/file.db
	if strings.HasPrefix(path, "s3://") {
		path = strings.TrimLeft(path, "s3://")
		return s3.New(path, &s3.Options{})
	}

	// gcs://user:pass@bucket/path/to/file.db
	if strings.HasPrefix(path, "gcs://") {
		path = strings.TrimLeft(path, "gcs://")
		return gcs.New(path, &gcs.Options{})
	}

	// logr://path/to/folder/with/file.db
	if strings.HasPrefix(path, "logr://") {
		path = strings.TrimLeft(path, "logr://")
		return logr.New(path, &logr.Options{})
	}

	// /path/to/file.db
	return file.New(path)

}
