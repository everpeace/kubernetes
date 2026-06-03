/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"
)

// We set MaxElements in AttributeType(cel.DeclType) to the larger of these two values
// when DRAListTypeAttributes feature gate is enabled because we cannot
// determine statically whether an attribute will be a scalar or a list at compile time.
// See newCompiler() for more details.
const maxElementsListTypeEnabled = uint64(max(resourceapi.DeviceAttributeMaxValueLength, resourceapi.ResourceSliceMaxAttributeValuesPerDevice))

var testcases = map[string]struct {
	// environment.StoredExpressions is the default (= all CEL fields and features from the current version available).
	// environment.NewExpressions can be used to enforce that only fields and features from the previous version are available.
	envType *environment.Type
	// The feature gate only has an effect in combination with environment.NewExpressions.
	enableConsumableCapacity bool
	// if enableListTypeAttributes is nil, the test will be run twice: once with it set to false and once with it set to true.
	// This allows testing both the old and new behavior of CEL expressions without having to duplicate all test cases.
	// If set, it controls whether the test is run with list-type attributes enabled or not.
	enableListTypeAttributes *bool
	expression               string
	driver                   string
	allowMultipleAllocations *bool
	attributes               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute
	capacity                 map[resourceapi.QualifiedName]resourceapi.DeviceCapacity
	expectCompileError       string
	expectMatchError         string
	expectMatch              bool

	// There's no good way to verify that the cost of an expression
	// really is what it should be other than eye-balling it. The
	// cost should not change in the future unless the expression
	// gets changed, so this provides some protection against
	// regressions.
	expectCost uint64
}{
	"true": {
		expression:  "true",
		expectMatch: true,
	},
	"false": {
		expression:  "false",
		expectMatch: false,
	},
	"syntax-error": {
		expression:         "?!",
		expectCompileError: "Syntax error",
	},
	"type-error": {
		expression:         `1`,
		expectCompileError: "must evaluate to bool or the unknown type, not int",
	},
	"runtime-error-lookup-identifier": {
		expression:       `device.attributes["no-such-domain"].noSuchAttr.isGreaterThan(quantity("0"))`,
		expectMatchError: "no such key: noSuchAttr",
		expectCost:       6,
	},
	"runtime-error-lookup-map": {
		expression:       `device.attributes["no-such-domain"]["noSuchAttr"].isGreaterThan(quantity("0"))`,
		expectMatchError: "no such key: noSuchAttr",
		expectCost:       6,
	},
	"domain-check-negative": {
		expression:  `"no-such-domain" in device.attributes`,
		expectMatch: false,
		expectCost:  3,
	},
	"domain-check-positive": {
		expression:  `"dra.example.com" in device.attributes`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"dra.example.com/something": {BoolValue: ptr.To(true)}},
		expectMatch: true,
		expectCost:  3,
	},
	"empty-driver-name": {
		expression:  `device.driver == ""`,
		expectMatch: true,
		expectCost:  2,
	},
	"real-driver-name": {
		expression:  `device.driver == "dra.example.com"`,
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  4,
	},
	"driver-name-qualifier": {
		expression:  `device.attributes["dra.example.com"].name`,
		driver:      "dra.example.com",
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: ptr.To(true)}},
		expectMatch: true,
		expectCost:  4,
	},
	"driver-name-qualifier-map-lookup": {
		expression:  `device.attributes["dra.example.com"]["name"]`,
		driver:      "dra.example.com",
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: ptr.To(true)}},
		expectMatch: true,
		expectCost:  4,
	},
	"bind": {
		expression:  `cel.bind(dra, device.attributes["dra.example.com"], dra.name)`,
		driver:      "dra.example.com",
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: ptr.To(true)}},
		expectMatch: true,
		expectCost:  15,
	},
	"qualified-attribute-name": {
		expression:  `device.attributes["other.example.com"].name`,
		driver:      "dra.example.com",
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"other.example.com/name": {BoolValue: ptr.To(true)}},
		expectMatch: true,
		expectCost:  4,
	},
	"bool": {
		expression:  `device.attributes["dra.example.com"].name`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: ptr.To(true)}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  4,
	},
	"int": {
		expression:  `device.attributes["dra.example.com"].name > 0`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValue: ptr.To(int64(1))}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  5,
	},
	"string": {
		expression:  `device.attributes["dra.example.com"].name == "fish"`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValue: ptr.To("fish")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  5,
	},
	"macro-exists-on-int": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.exists(x, x > 0)`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValue: new(int64(1))}},
		driver:                   "dra.example.com",
		expectMatchError:         "got 'types.Int', expected iterable type",
		expectCost:               5 + ((3 + 3) * maxElementsListTypeEnabled /* (cost(loopCondition=="not_strictly_false(!accu)") + cost(loopStep=="accu && (x > 0)")) * maxElementsListTypeEnabled */),
	},
	"macro-exists-on-bool": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.exists(x, x == true)`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: new(true)}},
		driver:                   "dra.example.com",
		expectMatchError:         "got 'types.Bool', expected iterable type",
		expectCost:               5 + ((3 + 3) * maxElementsListTypeEnabled /* (cost(loopCondition=="not_strictly_false(!accu)") + cost(loopStep=="accu && (x > 0)")) * maxElementsListTypeEnabled */),
	},
	"macro-exists-on-string": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.exists(x, x == "fish")`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValue: new("fish")}},
		driver:                   "dra.example.com",
		expectMatchError:         "got 'types.String', expected iterable type",
		expectCost:               5 + ((3 + 3) * maxElementsListTypeEnabled /* (cost(loopCondition=="not_strictly_false(!accu)") + cost(loopStep=="accu && (x > 0)")) * maxElementsListTypeEnabled */),
	},
	"attribute-access-causes-match-error": {
		enableListTypeAttributes: new(false),
		expression:               `device.attributes["dra.example.com"].names`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {StringValues: []string{"fish", "bird"}}},
		driver:                   "dra.example.com",
		expectMatchError:         "attribute names: unsupported attribute value",
		expectCost:               4,
	},
	"list-of-bool": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].names`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {BoolValues: []bool{true, false}}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               4,
	},
	"list-of-int": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].names > 0`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {IntValues: []int64{1, 2}}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               5,
	},
	"list-of-string": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].names == "fish"`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {StringValues: []string{"fish", "bird"}}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               5,
	},
	"list-of-semver": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].names.isGreaterThan(semver("0.0.1"))`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {VersionValues: []string{"1.0.0", "2.0.0"}}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               6,
	},
	"macro-on-list-of-int": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].names.asList().exists(x, x > 0)`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {IntValues: []int64{1, 2}}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               390,
	},
	"macro-on-list-of-string": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].names.asList().all(x, x != "")`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {StringValues: []string{"fish", "bird"}}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               262,
	},
	"macro-on-list-of-semver": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].names.asList().all(x, x.isGreaterThan(semver("0.0.1")))`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {VersionValues: []string{"1.0.0", "2.0.0"}}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               390,
	},
	"includes-function-undeclared-error": {
		enableConsumableCapacity: false,
		enableListTypeAttributes: new(false),
		// "includes" is injected via VersionedOptions.FeatureEnabled.
		// Use NewExpressions here because normal StoredExpressions ignores VersionedOptions.FeatureEnabled.
		envType:            ptr.To(environment.NewExpressions),
		expression:         `device.attributes["dra.example.com"].name.includes("fish")`,
		driver:             "dra.example.com",
		expectCompileError: "undeclared reference to 'includes'",
	},
	"includes-function-on-bool-scalar-positive": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes(true)`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: new(true)}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               4 + 48, /* cost of "includes" is max list length */
	},
	"includes-function-on-bool-scalar-negative": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes(true)`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: new(false)}},
		driver:                   "dra.example.com",
		expectMatch:              false,
		expectCost:               4 + 48, /* cost of "includes" is max list length */
	},
	"includes-function-on-bool-list-positive": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes(true)`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValues: []bool{true, false}}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               4 + 48, /* cost of "includes" is max list length */
	},
	"includes-function-on-bool-list-negative": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes(false)`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValues: []bool{true, true}}},
		driver:                   "dra.example.com",
		expectMatch:              false,
		expectCost:               4 + 48, /* cost of "includes" is max list length */
	},
	"includes-function-on-int-scalar-positive": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes(1)`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValue: new(int64(1))}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               4 + 48, /* cost of "includes" is max list length */
	},
	"includes-function-on-int-scalar-negative": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes(1)`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValue: new(int64(2))}},
		driver:                   "dra.example.com",
		expectMatch:              false,
		expectCost:               4 + 48, /* cost of "includes" is max list length */
	},
	"includes-function-on-int-list-positive": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes(1)`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValues: []int64{1, 2}}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               4 + 48, /* cost of "includes" is max list length */
	},
	"includes-function-on-int-list-negative": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes(3)`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValues: []int64{1, 2}}},
		driver:                   "dra.example.com",
		expectMatch:              false,
		expectCost:               4 + 48, /* cost of "includes" is max list length */
	},
	"includes-function-on-string-scalar-positive": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes("fish")`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValue: new("fish")}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               4 + 48, /* cost of "includes" is max list length */
	},
	"includes-function-on-string-scalar-negative": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes("bird")`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValue: new("fish")}},
		driver:                   "dra.example.com",
		expectMatch:              false,
		expectCost:               4 + 48, /* cost of "includes" is max list length */
	},
	"includes-function-on-string-list-positive": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes("fish")`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValues: []string{"fish", "bird"}}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               4 + 48, /* cost of "includes" is max list length */
	},
	"includes-function-on-string-list-negative": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes("cat")`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValues: []string{"fish", "bird"}}},
		driver:                   "dra.example.com",
		expectMatch:              false,
		expectCost:               4 + 48, /* cost of "includes" is max list length */
	},
	"includes-function-on-semver-scalar-positive": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes(semver("1.0.0"))`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValue: new("1.0.0")}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               4 + 48 /* cost of "includes" is max list length */ + 1,
	},
	"includes-function-on-semver-scalar-negative": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes(semver("2.0.0"))`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValue: new("1.0.0")}},
		driver:                   "dra.example.com",
		expectMatch:              false,
		expectCost:               4 + 48 /* cost of "includes" is max list length */ + 1,
	},
	"includes-function-on-semver-list-positive": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes(semver("1.0.0"))`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValues: []string{"1.0.0", "2.0.0"}}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               4 + 48 /* cost of "includes" is max list length */ + 1,
	},
	"includes-function-on-semver-list-negative": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.includes(semver("3.0.0"))`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValues: []string{"1.0.0", "2.0.0"}}},
		driver:                   "dra.example.com",
		expectMatch:              false,
		expectCost:               4 + 48 /* cost of "includes" is max list length */ + 1,
	},
	"includes-function-on-very-long-list-positive": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               fmt.Sprintf(`device.attributes["dra.example.com"].name.includes("value-%d")`, resourceapi.ResourceSliceMaxAttributeValuesPerDevice),
		attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValues: func() []string {
			values := make([]string, resourceapi.ResourceSliceMaxAttributeValuesPerDevice)
			for i := range values {
				values[i] = fmt.Sprintf("value-%d", i)
			}
			return values
		}()}},
		driver:      "dra.example.com",
		expectMatch: false,
		expectCost:  4 + 48, /* cost of "includes" is max list length */
	},
	"includes-function-on-very-long-list-runtime-error-with-as-list": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               fmt.Sprintf(`device.attributes["dra.example.com"].name.asList().includes("value-%d")`, resourceapi.ResourceSliceMaxAttributeValuesPerDevice+1),
		attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValues: func() []string {
			values := make([]string, resourceapi.ResourceSliceMaxAttributeValuesPerDevice+1)
			for i := range values {
				values[i] = fmt.Sprintf("value-%d", i)
			}
			return values
		}()}},
		driver:           "dra.example.com",
		expectMatchError: fmt.Sprintf("'includes' function cannot be applied to lists longer than %d values", resourceapi.ResourceSliceMaxAttributeValuesPerDevice),
		expectCost:       53, /* asList + includes */
	},
	"in-operator-on-list": {
		// This case is for documenting purpose to present the difference of call cost estimation
		// between "in" operator and "includes" function.
		// The cost estimation of "includes" is based on resourceapi.ResourceSliceMaxAttributeValuesPerDevice
		// because it's designed for checking whether a value is included in an device attribute list.
		// Instead, the cost estimation of "in" operator is based on maxElementsListTypeEnabled
		// (MaxElements in AttributeType(cel.DeclType)) as this operator is CEL standard one.
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `1 in device.attributes["dra.example.com"].names.asList()`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {IntValues: []int64{1, 2, 3}}},
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               69,
	},
	"version": {
		expression:  `device.attributes["dra.example.com"].name.isGreaterThan(semver("0.0.1"))`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValue: ptr.To("1.0.0")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  6,
	},
	"version_v": {
		// Relaxed parsing with v prefix.
		expression:  `device.attributes["dra.example.com"].name.isGreaterThan(semver("v0.0.1", true)) && isSemver("v1.0.0", true)`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValue: ptr.To("1.0.0")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  7,
	},
	"macro-exists-on-version": {
		enableListTypeAttributes: new(true),
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.attributes["dra.example.com"].name.exists(x, x.isGreaterThan(semver("0.0.1")))`,
		attributes:               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValue: new("1.0.0")}},
		driver:                   "dra.example.com",
		expectMatchError:         "got 'cel.Semver', expected iterable type",
		expectCost:               5 + ((3 + 4) * maxElementsListTypeEnabled /* (cost(loopCondition=="not_strictly_false(!accu)") + cost(loopStep=="accu && (x.isGreaterThan(semver("0.0.1"))")) * maxElementsListTypeEnabled */),
	},
	"quantity": {
		expression:  `device.capacity["dra.example.com"].name.isGreaterThan(quantity("1Ki"))`,
		capacity:    map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{"name": {Value: resource.MustParse("1Mi")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  6,
	},
	"check-positive": {
		expression:  `"name" in device.capacity["dra.example.com"] && device.capacity["dra.example.com"].name.isGreaterThan(quantity("1Ki"))`,
		capacity:    map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{"name": {Value: resource.MustParse("1Mi")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  10,
	},
	"check-negative": {
		expression:  `!("name" in device.capacity["dra.example.com"]) || device.capacity["dra.example.com"].name.isGreaterThan(quantity("1Ki"))`,
		expectMatch: true,
		expectCost:  11,
	},
	"all": {
		expression: `
device.capacity["dra.example.com"].quantity.isGreaterThan(quantity("1Ki")) &&
device.attributes["dra.example.com"].bool &&
device.attributes["dra.example.com"].int > 0 &&
device.attributes["dra.example.com"].string == "fish" &&
device.attributes["dra.example.com"].version.isGreaterThan(semver("0.0.1")) &&
device.capacity["dra.example.com"]["quantity"].isGreaterThan(quantity("1Ki")) &&
device.attributes["dra.example.com"]["bool"] &&
device.attributes["dra.example.com"]["int"] > 0 &&
device.attributes["dra.example.com"]["string"] == "fish" &&
device.attributes["dra.example.com"]["version"].isGreaterThan(semver("0.0.1"))
`,

		attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"bool":    {BoolValue: ptr.To(true)},
			"int":     {IntValue: ptr.To(int64(1))},
			"string":  {StringValue: ptr.To("fish")},
			"version": {VersionValue: ptr.To("1.0.0")},
		},
		capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			"quantity": {Value: resource.MustParse("1Mi")},
		},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  52,
	},
	"many": {
		expression: `device.attributes["dra.example.com"].a && device.attributes["dra.example.com"].b && device.attributes["dra.example.com"].c`,
		attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"a": {BoolValue: ptr.To(true)},
			"b": {BoolValue: ptr.To(true)},
			"c": {BoolValue: ptr.To(true)},
		},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  12,
	},
	"check_attribute_domains": {
		expression:  `device.attributes.exists_one(x, x == "dra.example.com")`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"services": {StringValue: ptr.To("some_example_value")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  164,
	},
	"check_attribute_ids": {
		expression:  `device.attributes["dra.example.com"].exists_one(x, x == "services")`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"services": {StringValue: ptr.To("some_example_value")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  133,
	},
	"split_attribute": {
		expression:  `device.attributes["dra.example.com"].services.split("example").size() >= 2`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"services": {StringValue: ptr.To("some_example_value")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  19,
	},
	"regexp_attribute": {
		expression:  `device.attributes["dra.example.com"].services.matches("[^a]?sym")`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"services": {StringValue: ptr.To("asymetric")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  18,
	},
	"check_capacity_domains": {
		expression:  `device.capacity.exists_one(x, x == "dra.example.com")`,
		capacity:    map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{"memory": {Value: resource.MustParse("1Mi")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  164,
	},
	"check_capacity_ids": {
		expression:  `device.capacity["dra.example.com"].exists_one(x, x == "memory")`,
		capacity:    map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{"memory": {Value: resource.MustParse("1Mi")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  133,
	},
	"expensive": {
		// The worst-case is based on the maximum number of
		// attributes and the maximum attribute name length.
		// To actually reach that expected cost at runtime, we must
		// have many attributes.
		attributes: func() map[resourceapi.QualifiedName]resourceapi.DeviceAttribute {
			attributes := make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute)
			prefix := "dra.example.com/"
			attribute := resourceapi.DeviceAttribute{
				StringValue: ptr.To("abc"),
			}
			for i := 0; i < resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice; i++ {
				suffix := fmt.Sprintf("-%d", i)
				name := prefix + strings.Repeat("x", resourceapi.DeviceMaxIDLength-len(suffix)) + suffix
				attributes[resourceapi.QualifiedName(name)] = attribute
			}
			return attributes
		}(),
		// From https://github.com/kubernetes/kubernetes/blob/50fc400f178d2078d0ca46aee955ee26375fc437/test/integration/apiserver/cel/validatingadmissionpolicy_test.go#L2150.
		expression:       `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(x, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(y, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z2, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z3, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z5, int('1'.find('[0-9]*')) < 100)))))))`,
		driver:           "dra.example.com",
		expectMatchError: "actual cost limit exceeded",
		expectCost:       85555551, // Exceed limit!
	},
	"allow_multiple_allocations": {
		enableConsumableCapacity: true,
		expression:               `device.allowMultipleAllocations == true`,
		allowMultipleAllocations: ptr.To(true),
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               3,
	},
	"allow_multiple_allocations_default": {
		enableConsumableCapacity: true,
		expression:               `device.allowMultipleAllocations == false`,
		allowMultipleAllocations: nil,
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               3,
	},
	"allow_multiple_allocations_false": {
		enableConsumableCapacity: true,
		expression:               `device.allowMultipleAllocations == false`,
		allowMultipleAllocations: ptr.To(false),
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               3,
	},
	"allow_multiple_allocations_new": {
		enableConsumableCapacity: true,
		envType:                  ptr.To(environment.NewExpressions),
		expression:               `device.allowMultipleAllocations == false`,
		allowMultipleAllocations: ptr.To(false),
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               3,
	},
	"allow_multiple_allocations_enabled": {
		envType:                  ptr.To(environment.NewExpressions),
		enableConsumableCapacity: true,
		expression:               `device.allowMultipleAllocations == false`,
		allowMultipleAllocations: ptr.To(false),
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               3,
	},
	"allow_multiple_allocations_disabled": {
		envType:                  ptr.To(environment.NewExpressions),
		enableConsumableCapacity: false,
		expression:               `device.allowMultipleAllocations == false`,
		driver:                   "dra.example.com",
		expectCompileError:       `undefined field 'allowMultipleAllocations'`,
	},
}

func TestCEL(t *testing.T) {
	for name, scenario := range testcases {
		run := func(t *testing.T, enableListTypeAttributes bool) {
			_, ctx := ktesting.NewTestContext(t)
			result := GetCompiler(Features{
				EnableConsumableCapacity: scenario.enableConsumableCapacity,
				EnableListTypeAttributes: enableListTypeAttributes,
			}).CompileCELExpression(scenario.expression, Options{EnvType: scenario.envType})
			if scenario.expectCompileError != "" && result.Error == nil {
				t.Fatalf("FAILURE: expected compile error %q, got none", scenario.expectCompileError)
			}
			if result.Error != nil {
				if scenario.expectCompileError == "" {
					t.Fatalf("FAILURE: unexpected compile error: %v", result.Error)
				}
				if !strings.Contains(result.Error.Error(), scenario.expectCompileError) {
					t.Fatalf("FAILURE: expected compile error to contain %q, but got instead: %v", scenario.expectCompileError, result.Error)
				}
				return
			}
			if scenario.expectCompileError != "" {
				t.Fatalf("FAILURE: expected compile error %q, got none", scenario.expectCompileError)
			}
			if expect, actual := scenario.expectCost, result.MaxCost; expect != actual {
				t.Errorf("ERROR: expected CEL cost %d, got %d instead (%.0f%% of limit %d)", expect, actual, float64(actual)*100.0/float64(resourceapi.CELSelectorExpressionMaxCost), resourceapi.CELSelectorExpressionMaxCost)
			}

			match, details, err := result.DeviceMatches(ctx, Device{
				AllowMultipleAllocations: scenario.allowMultipleAllocations, Attributes: scenario.attributes, Capacity: scenario.capacity, Driver: scenario.driver,
			})
			// details.ActualCost can be called for nil details, no need to check.
			actualCost := ptr.Deref(details.ActualCost(), 0)
			if scenario.expectCost > 0 {
				t.Logf("actual cost %d, %d%% of worst-case estimate %d", actualCost, actualCost*100/scenario.expectCost, scenario.expectCost)
			} else {
				t.Logf("actual cost %d, expected zero costs", actualCost)
			}
			if actualCost > result.MaxCost {
				t.Errorf("ERROR: cost estimate %d underestimated the evaluation cost of %d", result.MaxCost, actualCost)
			}

			if err != nil {
				if scenario.expectMatchError == "" {
					t.Fatalf("FAILURE: unexpected evaluation error: %v", err)
				}
				if !strings.Contains(err.Error(), scenario.expectMatchError) {
					t.Fatalf("FAILURE: expected evaluation error to contain %q, but got instead: %v", scenario.expectMatchError, err)
				}
				return
			}
			if scenario.expectMatchError != "" {
				t.Fatalf("FAILURE: expected match error %q, got none", scenario.expectMatchError)
			}
			if match != scenario.expectMatch {
				t.Fatalf("FAILURE: expected result %v, got %v", scenario.expectMatch, match)
			}
		}
		if scenario.enableListTypeAttributes == nil {
			t.Run(name+"-list-type-attributes-false", func(t *testing.T) {
				run(t, false)
			})
			t.Run(name+"-list-type-attributes-true", func(t *testing.T) {
				run(t, true)
			})
		} else {
			t.Run(fmt.Sprintf("%s-list-type-attributes-%v", name, *scenario.enableListTypeAttributes), func(t *testing.T) {
				run(t, *scenario.enableListTypeAttributes)
			})
		}
	}
}

func TestListTypeAttributeSemantics(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	eval := func(t *testing.T, expression string, attributes map[resourceapi.QualifiedName]resourceapi.DeviceAttribute) (bool, error) {
		t.Helper()
		result := GetCompiler(Features{EnableListTypeAttributes: true}).CompileCELExpression(expression, Options{
			EnvType:               ptr.To(environment.NewExpressions),
			DisableCostEstimation: true,
		})
		if result.Error != nil {
			t.Fatalf("unexpected compile error for %q: %v", expression, result.Error)
		}

		match, _, err := result.DeviceMatches(ctx, Device{
			Driver:     "foo",
			Attributes: attributes,
		})
		return match, err
	}

	type testCase struct {
		name        string
		expression  string
		attributes  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute
		wantMatch   bool
		wantErrLike string
	}

	testCases := []testCase{
		// device.attributes["foo"].attributeName: scalar -> that value, list -> first value.
		{
			name:       "attribute-access-bool-scalar",
			expression: `device.attributes["foo"].name`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: ptr.To(true)}},
			wantMatch:  true,
		},
		{
			name:       "attribute-access-bool-list-uses-first",
			expression: `device.attributes["foo"].name`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValues: []bool{false, true}}},
			wantMatch:  false,
		},
		{
			name:       "attribute-access-int-scalar",
			expression: `device.attributes["foo"].name > 6 && device.attributes["foo"].name < 8`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValue: ptr.To(int64(7))}},
			wantMatch:  true,
		},
		{
			name:       "attribute-access-int-list-uses-first",
			expression: `device.attributes["foo"].name > 6 && device.attributes["foo"].name < 8`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValues: []int64{7, 9}}},
			wantMatch:  true,
		},
		{
			name:       "attribute-access-string-scalar",
			expression: `device.attributes["foo"].name == "fish"`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValue: ptr.To("fish")}},
			wantMatch:  true,
		},
		{
			name:       "attribute-access-string-list-uses-first",
			expression: `device.attributes["foo"].name == "fish"`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValues: []string{"fish", "bird"}}},
			wantMatch:  true,
		},
		{
			name:       "attribute-access-semver-scalar",
			expression: `device.attributes["foo"].name.isGreaterThan(semver("1.1.0"))`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValue: ptr.To("1.2.0")}},
			wantMatch:  true,
		},
		{
			name:       "attribute-access-semver-list-uses-first",
			expression: `device.attributes["foo"].name.isGreaterThan(semver("1.1.0"))`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValues: []string{"1.2.0", "2.0.0"}}},
			wantMatch:  true,
		},

		// device.attributes["foo"].attributeName.asList(): scalar -> singleton, list -> original list.
		{
			name:       "as-list-bool-scalar-singleton",
			expression: `device.attributes["foo"].name.asList().size() == 1 && device.attributes["foo"].name.asList()[0] == true`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: ptr.To(true)}},
			wantMatch:  true,
		},
		{
			name:       "as-list-bool-list-original",
			expression: `device.attributes["foo"].name.asList().size() == 2 && device.attributes["foo"].name.asList()[1] == true`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValues: []bool{false, true}}},
			wantMatch:  true,
		},
		{
			name:       "as-list-int-scalar-singleton",
			expression: `device.attributes["foo"].name.asList().size() == 1 && device.attributes["foo"].name.asList()[0] == 7`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValue: ptr.To(int64(7))}},
			wantMatch:  true,
		},
		{
			name:       "as-list-int-list-original",
			expression: `device.attributes["foo"].name.asList().size() == 2 && device.attributes["foo"].name.asList()[1] == 9`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValues: []int64{7, 9}}},
			wantMatch:  true,
		},
		{
			name:       "as-list-string-scalar-singleton",
			expression: `device.attributes["foo"].name.asList().size() == 1 && device.attributes["foo"].name.asList()[0] == "fish"`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValue: ptr.To("fish")}},
			wantMatch:  true,
		},
		{
			name:       "as-list-string-list-original",
			expression: `device.attributes["foo"].name.asList().size() == 2 && device.attributes["foo"].name.asList()[1] == "bird"`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValues: []string{"fish", "bird"}}},
			wantMatch:  true,
		},
		{
			name:       "as-list-semver-scalar-singleton",
			expression: `device.attributes["foo"].name.asList().size() == 1 && device.attributes["foo"].name.asList()[0].isGreaterThan(semver("1.1.0"))`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValue: ptr.To("1.2.0")}},
			wantMatch:  true,
		},
		{
			name:       "as-list-semver-list-original",
			expression: `device.attributes["foo"].name.asList().size() == 2 && device.attributes["foo"].name.asList()[1].isGreaterThan(semver("1.9.0"))`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValues: []string{"1.2.0", "2.0.0"}}},
			wantMatch:  true,
		},

		// device.attributes["foo"].includes(x): scalar -> scalar match, list -> first-element match.
		{
			name:       "includes-bool-scalar-true",
			expression: `device.attributes["foo"].name.includes(true)`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: ptr.To(true)}},
			wantMatch:  true,
		},
		{
			name:       "includes-bool-scalar-false",
			expression: `device.attributes["foo"].name.includes(false)`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: ptr.To(true)}},
			wantMatch:  false,
		},
		{
			name:       "includes-bool-list-first-true",
			expression: `device.attributes["foo"].name.includes(true)`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValues: []bool{true, false}}},
			wantMatch:  true,
		},
		{
			name:       "includes-bool-list-first-false",
			expression: `device.attributes["foo"].name.includes(true)`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValues: []bool{false, true}}},
			wantMatch:  false,
		},
		{
			name:       "includes-int-list-matches-first-only",
			expression: `device.attributes["foo"].name.includes(9)`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValues: []int64{7, 9}}},
			wantMatch:  false,
		},
		{
			name:       "includes-string-list-matches-first-only",
			expression: `device.attributes["foo"].name.includes("bird")`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValues: []string{"fish", "bird"}}},
			wantMatch:  false,
		},
		{
			name:       "includes-semver-list-matches-first-only",
			expression: `device.attributes["foo"].name.includes(semver("2.0.0"))`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValues: []string{"1.2.0", "2.0.0"}}},
			wantMatch:  false,
		},

		// device.attributes["foo"].asList().includes(x): scalar -> scalar match, list -> full-list containment.
		{
			name:       "as-list-includes-bool-list-searches-all",
			expression: `device.attributes["foo"].name.asList().includes(true)`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValues: []bool{false, true}}},
			wantMatch:  true,
		},
		{
			name:       "as-list-includes-int-list-searches-all",
			expression: `device.attributes["foo"].name.asList().includes(9)`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValues: []int64{7, 9}}},
			wantMatch:  true,
		},
		{
			name:       "as-list-includes-string-list-searches-all",
			expression: `device.attributes["foo"].name.asList().includes("bird")`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValues: []string{"fish", "bird"}}},
			wantMatch:  true,
		},
		{
			name:       "as-list-includes-semver-list-searches-all",
			expression: `device.attributes["foo"].name.asList().includes(semver("2.0.0"))`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValues: []string{"1.2.0", "2.0.0"}}},
			wantMatch:  true,
		},

		// Macros: attributeName errors, attributeName.asList() works.
		{
			name:        "macro-on-bool-list-without-as-list-errors",
			expression:  `device.attributes["foo"].name.exists(x, x == true)`,
			attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValues: []bool{true, false}}},
			wantErrLike: "expected iterable type",
		},
		{
			name:       "macro-on-bool-list-with-as-list-works",
			expression: `device.attributes["foo"].name.asList().exists(x, x == true)`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValues: []bool{true, false}}},
			wantMatch:  true,
		},
		{
			name:        "macro-on-int-list-without-as-list-errors",
			expression:  `device.attributes["foo"].name.exists(x, x > 0)`,
			attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValues: []int64{7, 9}}},
			wantErrLike: "expected iterable type",
		},
		{
			name:       "macro-on-int-list-with-as-list-works",
			expression: `device.attributes["foo"].name.asList().exists(x, x > 0)`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValues: []int64{7, 9}}},
			wantMatch:  true,
		},
		{
			name:        "macro-on-string-list-without-as-list-errors",
			expression:  `device.attributes["foo"].name.all(x, x != "")`,
			attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValues: []string{"fish", "bird"}}},
			wantErrLike: "expected iterable type",
		},
		{
			name:       "macro-on-string-list-with-as-list-works",
			expression: `device.attributes["foo"].name.asList().all(x, x != "")`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValues: []string{"fish", "bird"}}},
			wantMatch:  true,
		},
		{
			name:        "macro-on-semver-list-without-as-list-errors",
			expression:  `device.attributes["foo"].name.exists(x, x.isGreaterThan(semver("0.0.1")))`,
			attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValues: []string{"1.2.0", "2.0.0"}}},
			wantErrLike: "expected iterable type",
		},
		{
			name:       "macro-on-semver-list-with-as-list-works",
			expression: `device.attributes["foo"].name.asList().exists(x, x.isGreaterThan(semver("0.0.1")))`,
			attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValues: []string{"1.2.0", "2.0.0"}}},
			wantMatch:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			match, err := eval(t, tc.expression, tc.attributes)
			if tc.wantErrLike != "" {
				if err == nil {
					t.Fatalf("expected evaluation error containing %q, got none", tc.wantErrLike)
				}
				if !strings.Contains(err.Error(), tc.wantErrLike) {
					t.Fatalf("expected evaluation error containing %q, got %v", tc.wantErrLike, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected evaluation error: %v", err)
			}
			if match != tc.wantMatch {
				t.Fatalf("expected match %v, got %v", tc.wantMatch, match)
			}
		})
	}
}

func TestInterrupt(t *testing.T) {
	for _, name := range []string{"timeout", "deadline", "cancel"} {
		t.Run(name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			// Adapted from https://github.com/kubernetes/kubernetes/blob/e0859f91b7d269bb7e2f43e23d202ccccaf34c0c/staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel/validation_test.go#L3006
			expression := `device.attributes["dra.example.com"].map(key, device.attributes["dra.example.com"][key] * 20).filter(e, e > 50).exists(e, e == 60)`
			result := GetCompiler(Features{}).CompileCELExpression(expression, Options{})
			if result.Error != nil {
				t.Fatalf("unexpected compile error: %v", result.Error)
			}
			switch name {
			case "timeout":
				c, cancel := context.WithTimeout(ctx, time.Nanosecond)
				defer cancel()
				ctx = c
			case "deadline":
				c, cancel := context.WithDeadline(ctx, time.Now())
				defer cancel()
				ctx = c
			case "cancel":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			device := Device{
				Attributes: make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute),
			}
			for i := int64(0); i < 1000; i++ {
				device.Attributes[resourceapi.QualifiedName(fmt.Sprintf("dra.example.com/attr%d", i))] = resourceapi.DeviceAttribute{
					IntValue: ptr.To(i),
				}
			}
			_, _, err := result.DeviceMatches(ctx, device)
			if ctx.Err() != nil {
				if !errors.Is(err, ctx.Err()) {
					t.Fatalf("expected %v, got error: %v", ctx.Err(), err)
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			}
		})
	}
}

func BenchmarkDeviceMatches(b *testing.B) {
	for name, scenario := range testcases {
		if scenario.expectCompileError != "" {
			continue
		}

		run := func(b *testing.B, enableListTypeAttributes bool) {
			_, ctx := ktesting.NewTestContext(b)
			result := GetCompiler(Features{EnableListTypeAttributes: enableListTypeAttributes}).CompileCELExpression(scenario.expression, Options{})
			if result.Error != nil {
				b.Fatalf("unexpected compile error: %s", result.Error.Error())
			}

			for i := 0; i < b.N; i++ {
				// It would be nice to measure
				// time/actual_cost, but the time as observed
				// here also includes additional preparations
				// in result.DeviceMatches and thus cannot be
				// used.
				match, _, err := result.DeviceMatches(ctx, Device{
					AllowMultipleAllocations: scenario.allowMultipleAllocations, Attributes: scenario.attributes, Capacity: scenario.capacity, Driver: scenario.driver,
				})
				if err != nil {
					if scenario.expectMatchError == "" {
						b.Fatalf("unexpected evaluation error: %v", err)
					}
					if !strings.Contains(err.Error(), scenario.expectMatchError) {
						b.Fatalf("expected evaluation error to contain %q, but got instead: %v", scenario.expectMatchError, err)
					}
					return
				}
				if scenario.expectMatchError != "" {
					b.Fatalf("expected match error %q, got none", scenario.expectMatchError)
				}
				if match != scenario.expectMatch {
					b.Fatalf("expected result %v, got %v", scenario.expectMatch, match)
				}
			}
		}
		if scenario.enableListTypeAttributes == nil {
			b.Run(name+"-list-type-attributes-false", func(b *testing.B) {
				run(b, false)
			})
			b.Run(name+"-list-type-attributes-true", func(b *testing.B) {
				run(b, true)
			})
		} else {
			b.Run(fmt.Sprintf("%s-list-type-attributes-%v", name, *scenario.enableListTypeAttributes), func(b *testing.B) {
				run(b, *scenario.enableListTypeAttributes)
			})
		}
	}
}
