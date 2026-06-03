package cel

import (
	"reflect"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// listBackedScalarValue behaves like a scalar for normal CEL operations,
// but keeps the original list for explicit list-oriented helpers like asList.
type listBackedScalarValue interface {
	ref.Val
	asList() traits.Lister
}

type listBackedScalar struct {
	scalar ref.Val
	list   traits.Lister
}

func newListBackedScalar(scalar any, list any) ref.Val {
	return listBackedScalar{
		scalar: types.DefaultTypeAdapter.NativeToValue(scalar),
		list:   types.NewDynamicList(types.DefaultTypeAdapter, list),
	}
}

func (v listBackedScalar) asList() traits.Lister {
	return v.list
}

func (v listBackedScalar) ConvertToNative(typeDesc reflect.Type) (any, error) {
	return v.scalar.ConvertToNative(typeDesc)
}

func (v listBackedScalar) ConvertToType(typeValue ref.Type) ref.Val {
	return v.scalar.ConvertToType(typeValue)
}

func (v listBackedScalar) Equal(other ref.Val) ref.Val {
	return v.scalar.Equal(other)
}

func (v listBackedScalar) Type() ref.Type {
	return v.scalar.Type()
}

func (v listBackedScalar) Value() any {
	return v.scalar.Value()
}

func (v listBackedScalar) Add(other ref.Val) ref.Val {
	adder, ok := v.scalar.(traits.Adder)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	return adder.Add(other)
}

func (v listBackedScalar) Compare(other ref.Val) ref.Val {
	comparer, ok := v.scalar.(traits.Comparer)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	return comparer.Compare(other)
}

func (v listBackedScalar) Contains(value ref.Val) ref.Val {
	container, ok := v.scalar.(traits.Container)
	if !ok {
		return types.MaybeNoSuchOverloadErr(value)
	}
	return container.Contains(value)
}

func (v listBackedScalar) Divide(denominator ref.Val) ref.Val {
	divider, ok := v.scalar.(traits.Divider)
	if !ok {
		return types.MaybeNoSuchOverloadErr(denominator)
	}
	return divider.Divide(denominator)
}

func (v listBackedScalar) Get(index ref.Val) ref.Val {
	indexer, ok := v.scalar.(traits.Indexer)
	if !ok {
		return types.MaybeNoSuchOverloadErr(index)
	}
	return indexer.Get(index)
}

func (v listBackedScalar) Match(pattern ref.Val) ref.Val {
	matcher, ok := v.scalar.(traits.Matcher)
	if !ok {
		return types.MaybeNoSuchOverloadErr(pattern)
	}
	return matcher.Match(pattern)
}

func (v listBackedScalar) Modulo(denominator ref.Val) ref.Val {
	modder, ok := v.scalar.(traits.Modder)
	if !ok {
		return types.MaybeNoSuchOverloadErr(denominator)
	}
	return modder.Modulo(denominator)
}

func (v listBackedScalar) Multiply(other ref.Val) ref.Val {
	multiplier, ok := v.scalar.(traits.Multiplier)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	return multiplier.Multiply(other)
}

func (v listBackedScalar) Negate() ref.Val {
	negater, ok := v.scalar.(traits.Negater)
	if !ok {
		return types.NoSuchOverloadErr()
	}
	return negater.Negate()
}

func (v listBackedScalar) Receive(function string, overload string, args []ref.Val) ref.Val {
	receiver, ok := v.scalar.(traits.Receiver)
	if !ok {
		return types.NoSuchOverloadErr()
	}
	return receiver.Receive(function, overload, args)
}

func (v listBackedScalar) Size() ref.Val {
	sizer, ok := v.scalar.(traits.Sizer)
	if !ok {
		return types.NoSuchOverloadErr()
	}
	return sizer.Size()
}

func (v listBackedScalar) Subtract(subtrahend ref.Val) ref.Val {
	subtractor, ok := v.scalar.(traits.Subtractor)
	if !ok {
		return types.MaybeNoSuchOverloadErr(subtrahend)
	}
	return subtractor.Subtract(subtrahend)
}
