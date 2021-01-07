package conjungo

import (
	"fmt"
	"reflect"
)

// A MergeFunc defines how two items are merged together. It should accept a reflect.Value
// representation of a target and source, and return the final merged product.
// The value returned from the function will be written directly to the parent value,
// as long as there is no error.
// Options are also passed in, and it is the responsibility of the function to honor
// these options and handle any variations in behavior that should occur.
type MergeFunc func(target, source reflect.Value, o *Options) (reflect.Value, error)

type funcSelector struct {
	typeFuncs      map[reflect.Type]MergeFunc
	interfaceFuncs map[reflect.Type]MergeFunc
	kindFuncs      map[reflect.Kind]MergeFunc
	defaultFunc    MergeFunc
}

func newFuncSelector() *funcSelector {
	return &funcSelector{
		typeFuncs: map[reflect.Type]MergeFunc{
			reflect.TypeOf([]byte{}): mergeBytes,
		},
		interfaceFuncs: map[reflect.Type]MergeFunc{},
		kindFuncs: map[reflect.Kind]MergeFunc{
			reflect.Map:    mergeMap,
			reflect.Slice:  mergeSlice,
			reflect.Struct: mergeStruct,
			reflect.Ptr:    mergePtr,
		},
		defaultFunc: defaultMergeFunc,
	}
}

func (f *funcSelector) setTypeMergeFunc(t reflect.Type, mf MergeFunc) {
	if nil == f.typeFuncs {
		f.typeFuncs = map[reflect.Type]MergeFunc{}
	}
	f.typeFuncs[t] = mf
}

func (f *funcSelector) setInterfaceMergeFunc(t reflect.Type, mf MergeFunc) {
	if nil == f.interfaceFuncs {
		f.interfaceFuncs = map[reflect.Type]MergeFunc{}
	}
	f.interfaceFuncs[t] = mf
}

func (f *funcSelector) setKindMergeFunc(k reflect.Kind, mf MergeFunc) {
	if nil == f.kindFuncs {
		f.kindFuncs = map[reflect.Kind]MergeFunc{}
	}
	f.kindFuncs[k] = mf
}

func (f *funcSelector) setDefaultMergeFunc(mf MergeFunc) {
	f.defaultFunc = mf
}

// Get func must always return a function.
// First looks for a merge func defined for its type. Type is the most specific way to categorize something,
// for example, struct type foo of package bar or map[string]string. Next it looks for a merge func defined for its
// kind, for example, struct or map. At this point, if nothing matches, it will fall back to the default merge definition.
func (f *funcSelector) getFunc(v reflect.Value) MergeFunc {
	// prioritize a specific 'type' definition
	ti := v.Type()

	if fx, ok := f.typeFuncs[ti]; ok {
		return fx
	}

	// or look for if the type interface a type
	for impl, fx := range f.interfaceFuncs {
		if ti.Implements(impl) {
			return fx
		}
	}

	// then look for a more general 'kind'.
	if fx, ok := f.kindFuncs[ti.Kind()]; ok {
		return fx
	}

	if f.defaultFunc != nil {
		return f.defaultFunc
	}

	return defaultMergeFunc
}

// The most basic merge function to be used as default behavior.
// In overwrite mode, it returns the source. Otherwise, it returns the target.
func defaultMergeFunc(t, s reflect.Value, o *Options) (reflect.Value, error) {
	// Explicitly using IsZero due to: val.IsZero() != !val.IsValid() as used by isEmpty()
	// reflect.ValueOf("").IsZero() == true
	// reflect.ValueOf("").IsValid() == true
	if o.IgnoreEmpty && s.IsZero() {
		return t, nil
	}

	if o.Overwrite {
		return s, nil
	}

	return t, nil
}

// By default don't treat byte slices as a normal slice, they should not be appended like mergeSlice
// does, instead replaced if needed according to the default merge func
func mergeBytes(t, s reflect.Value, o *Options) (reflect.Value, error) {
	if s.Len() == 0 {
		return t, nil
	}

	if t.Len() == 0 {
		return s, nil
	}

	return defaultMergeFunc(t, s, o)
}

func mergeMap(t, s reflect.Value, o *Options) (v reflect.Value, err error) {
	if t.Kind() != reflect.Map || s.Kind() != reflect.Map {
		return reflect.Value{}, fmt.Errorf("got non-map type (tagret: %v; source: %v)", t.Kind(), s.Kind())
	}

	keys := s.MapKeys()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("failed to merge map: %v", r)
		}
	}()

	for _, k := range keys {
		val, err := merge(t.MapIndex(k), s.MapIndex(k), o)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("key '%s': %v", k, err)
		}
		t.SetMapIndex(k, val)
	}

	v = t
	return
}

// Merges two slices of the same type by appending source to target.
func mergeSlice(t, s reflect.Value, o *Options) (reflect.Value, error) {
	if t.Type() != s.Type() {
		return reflect.Value{}, fmt.Errorf("slices must have same type: T: %v S: %v", t.Type(), s.Type())
	}

	return reflect.AppendSlice(t, s), nil
}

// This func is designed to be called by merge().
// It should not be used on its own because it will panic.
func mergeStruct(t, s reflect.Value, o *Options) (reflect.Value, error) {
	// accept pointer values, but dereference them
	valT := reflect.Indirect(t)
	valS := reflect.Indirect(s)
	kindT := valT.Kind()
	kindS := valS.Kind()

	newT := reflect.New(valT.Type()).Elem() //a new instance of the struct type that can be set

	okT := kindT == reflect.Struct
	okS := kindS == reflect.Struct
	if !okT || !okS {
		return reflect.Value{}, fmt.Errorf("got non-struct kind (tagret: %v; source: %v)", kindT, kindS)
	}

	for i := 0; i < valS.NumField(); i++ {
		fieldT := newT.Field(i)

		// field is addressable because it's created above. So this means it is unexported.
		if !fieldT.CanSet() {
			if o.ErrorOnUnexported {
				return reflect.Value{}, fmt.Errorf("struct of type %v has unexported field: %s",
					t.Type().Name(), newT.Type().Field(i).Name)
			}

			// revert to using the default func instead to treat the struct as single entity
			return defaultMergeFunc(t, s, o)
		}

		//fieldT should always be valid because it's created above
		merged, err := merge(valT.Field(i), valS.Field(i), o)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("failed to merge field `%s.%s`: %v",
				newT.Type().Name(), newT.Type().Field(i).Name, err)
		}

		if !merged.IsValid() {
			// if merge returned an invalid value, fallback to a default merge for the field
			// defaultMergeFun() does not error
			merged, _ = defaultMergeFunc(valT.Field(i), valS.Field(i), o)
		}

		if fieldT.Kind() != reflect.Interface && fieldT.Type() != merged.Type() {
			return reflect.Value{}, fmt.Errorf("types dont match %v <> %v", fieldT.Type(), merged.Type())
		}

		fieldT.Set(merged)
	}

	return newT, nil
}

// This func is designed to be called by merge().
// It should not be used on its own because it will panic.
func mergePtr(t, s reflect.Value, o *Options) (reflect.Value, error) {
	valT := t.Elem()
	valS := s.Elem()
	newT := reflect.New(valT.Type())

	//newT.Elem() should always be valid because it's created above
	if !newT.Elem().CanSet() {
		if o.ErrorOnUnexported {
			return reflect.Value{}, fmt.Errorf("value type %v is unexported",
				newT.Type().Name())
		}

		// revert to using the default func instead to treat this value type as single entity
		return defaultMergeFunc(t, s, o)
	}

	merged, err := merge(valT, valS, o)
	if err != nil {
		return reflect.Value{}, err
	}

	newT.Elem().Set(merged)

	return newT, nil
}

// Custom func helper to merge a slice of structs or slice of pointers to struct by a specific field. The caller needs
// to make sure this field value is unique across the slice otherwise the first matching target is merged over again.
// Intended to be used to construct a type specific merge func to be added to the options.
func MergeSliceByField(field string) func(t, s reflect.Value, o *Options) (reflect.Value, error) {
	m := mergeSliceByField{field: field}

	return m.merge
}

type mergeSliceByField struct {
	field string
}

// nolint: goerr113, cognit
func (m mergeSliceByField) merge(t, s reflect.Value, o *Options) (reflect.Value, error) {
	// Test that dst kind is a slice
	if t.Kind() != reflect.Slice {
		return reflect.Value{}, fmt.Errorf("kind %v is not a slice", t.Kind())
	}

	var elemTypeT reflect.Type

	if t.Type().Elem().Kind() == reflect.Ptr {
		elemTypeT = t.Type().Elem().Elem()
	} else {
		elemTypeT = t.Type().Elem()
	}

	// Test that elements kind is a struct
	if elemTypeT.Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("element kind %v is not a struct", t.Type().Elem().Kind())
	}

	// Test that elements kind has even the provided field. Allowed to be a pointer to a struct
	if _, ok := elemTypeT.FieldByName(m.field); !ok {
		return reflect.Value{}, fmt.Errorf("field %v of element type %v does not exist", m.field, t.Type().Elem())
	}

	// Loop through both slices and try to find the field matching
Next:
	for i := 0; i < s.Len(); i++ {
		for j := 0; j < t.Len(); j++ {
			var elemValT, elemValS reflect.Value

			if t.Type().Elem().Kind() == reflect.Ptr {
				elemValT = t.Index(j).Elem()
				elemValS = s.Index(i).Elem()
			} else {
				elemValT = t.Index(j)
				elemValS = s.Index(i)
			}

			// TODO: we could also check if the type implements comparabale, so we don't have to deep equal, which would be faster for those types
			if reflect.DeepEqual(elemValT.FieldByName(m.field).Interface(), elemValS.FieldByName(m.field).Interface()) {
				// Intentionally we use the real index value again, as merge() is capable of taking a ptr or struct
				merged, err := merge(t.Index(j), s.Index(i), o)
				if err != nil {
					return reflect.Value{}, err
				}

				t.Index(j).Set(merged)

				continue Next
			}
		}

		// If never continued to Next, no match has been found and just append it
		t = reflect.Append(t, s.Index(i))
	}

	return t, nil
}
