// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"github.com/juju/errors"
)

type addFunc func(i interface{}) (*string, error)

func putList(items []interface{}, adder addFunc) (*string, error) {
	if len(items) == 0 {
		panic("list should have at least one item")
	}
	target := ""
	gen, err := adder(items[0])
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen
	if len(items) > 1 {
		for _, v := range items[1:] {
			target += ","
			gen, err := adder(v)
			if err != nil {
				return nil, errors.Trace(err)
			}
			target += *gen
		}
	}
	return &target, nil
}
